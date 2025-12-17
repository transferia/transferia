package synthetic_partition

import (
	"fmt"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/file"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/lr_window/l_window"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/ordered_multimap"
)

type SyntheticPartition struct {
	// parameter

	syntheticPartitionNum int

	// state

	beforeListTime time.Time
	listDuration   time.Duration

	queueToHandle *ordered_multimap.OrderedMultimap
	committed     *l_window.LWindow
}

func (f *SyntheticPartition) SyntheticPartitionNum() int {
	return f.syntheticPartitionNum
}

func (f *SyntheticPartition) SyntheticPartitionStr() string {
	return fmt.Sprintf("%d", f.syntheticPartitionNum)
}

//---------------------------------------------------------------------------------------------------------------------
// stateful part

func (f *SyntheticPartition) BeforeListing() error {
	f.beforeListTime = time.Now()
	if f.queueToHandle.Size() != 0 {
		return xerrors.Errorf("contract is broken - on 'listing' stage, queueToHandle should be empty")
	}
	return nil
}

func (f *SyntheticPartition) AfterListing() {
	f.listDuration = time.Since(f.beforeListTime)
}

func (f *SyntheticPartition) Files() []file.File {
	result := make([]file.File, 0, f.queueToHandle.Size()*2)
	f.queueToHandle.ForEach(func(key int64, value string) {
		result = append(result, file.File{
			FileName:       value,
			FileSize:       0,
			LastModifiedNS: key,
		})
	})
	return result
}

func (f *SyntheticPartition) AddIfNew(newFile *file.File) (bool, error) {
	if f.committed.IsNewFile(newFile.LastModifiedNS, newFile.FileName) {
		// ADD
		err := f.queueToHandle.Add(newFile.LastModifiedNS, newFile.FileName)
		if err != nil {
			return false, xerrors.Errorf("unable to add file into queueToHandle, err: %w", err)
		}
		return true, nil
	}

	// SKIP, we already committed it
	return false, nil
}

func (f *SyntheticPartition) Commit(fileName string) error {
	// find 'File' object
	currFileNS, err := f.queueToHandle.GetKeyByValue(fileName)
	if err != nil {
		return xerrors.Errorf("unable to get file, err: %w", err)
	}

	// remove 'File' from queueToHandle
	err = f.queueToHandle.RemoveValue(fileName)
	if err != nil {
		return xerrors.Errorf("unable to remove file, err: %w", err)
	}

	// commit
	err = f.committed.Commit(currFileNS, fileName, f.listDuration, f.queueToHandle)
	if err != nil {
		return xerrors.Errorf("unable to add file into committed, err: %w", err)
	}

	return nil
}

func (f *SyntheticPartition) CommitAll() error {
	f.queueToHandle.ForEachKey(func(key int64, values []string) {
		for _, value := range values {
			_ = f.committed.Commit(key, value, f.listDuration, f.queueToHandle)
		}
	})
	f.queueToHandle.Clear()
	return nil
}

func (f *SyntheticPartition) LastCommittedStateFromString(in string) error {
	return f.committed.Deserialize([]byte(in))
}

func (f *SyntheticPartition) LastCommittedStateToString() string {
	result, _ := f.committed.Serialize()
	return string(result)
}

//---------------------------------------------------------------------------------------------------------------------

func NewSyntheticPartition(syntheticPartitionNum int, overlapDuration time.Duration) *SyntheticPartition {
	return &SyntheticPartition{
		syntheticPartitionNum: syntheticPartitionNum,
		beforeListTime:        time.Time{},
		listDuration:          0,
		queueToHandle:         ordered_multimap.NewOrderedMultimap(),
		committed:             l_window.NewLWindow(overlapDuration),
	}
}
