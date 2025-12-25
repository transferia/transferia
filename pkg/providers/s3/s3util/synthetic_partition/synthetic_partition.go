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

	fullQueueToHandle *ordered_multimap.OrderedMultimap
	lWindow           *l_window.LWindow
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
	if f.fullQueueToHandle.Size() != 0 {
		return xerrors.Errorf("contract is broken - on 'listing' stage, fullQueueToHandle should be empty")
	}
	return nil
}

func (f *SyntheticPartition) AfterListing() {
	f.listDuration = time.Since(f.beforeListTime)
}

func (f *SyntheticPartition) Files() []file.File {
	result := make([]file.File, 0, f.fullQueueToHandle.Size()*2)
	f.fullQueueToHandle.ForEach(func(key int64, value string) {
		result = append(result, file.File{
			FileName:       value,
			FileSize:       0,
			LastModifiedNS: key,
		})
	})
	return result
}

func (f *SyntheticPartition) AddIfNew(newFile *file.File) (bool, error) {
	isNewFile, err := f.lWindow.IsNewFile(newFile.LastModifiedNS, newFile.FileName)
	if err != nil {
		return false, xerrors.Errorf("failed to determine if a new file is new: %w", err)
	}
	if isNewFile {
		// ADD
		err := f.fullQueueToHandle.Add(newFile.LastModifiedNS, newFile.FileName)
		if err != nil {
			return false, xerrors.Errorf("unable to add file into fullQueueToHandle, err: %w", err)
		}
		return true, nil
	}

	// SKIP, we already lWindow it
	return false, nil
}

func (f *SyntheticPartition) Commit(fileName string) error {
	// find 'File' object
	currFileNS, err := f.fullQueueToHandle.GetKeyByValue(fileName)
	if err != nil {
		return xerrors.Errorf("unable to get file, err: %w", err)
	}

	// remove 'File' from fullQueueToHandle
	err = f.fullQueueToHandle.RemoveValue(fileName)
	if err != nil {
		return xerrors.Errorf("unable to remove file, err: %w", err)
	}

	// commit
	err = f.lWindow.Commit(currFileNS, fileName, f.listDuration)
	if err != nil {
		return xerrors.Errorf("unable to add file into lWindow, err: %w", err)
	}

	return nil
}

func (f *SyntheticPartition) CommitAll() error {
	f.fullQueueToHandle.ForEachKey(func(key int64, values []string) {
		for _, value := range values {
			_ = f.lWindow.Commit(key, value, f.listDuration)
		}
	})
	f.fullQueueToHandle.Clear()
	return nil
}

func (f *SyntheticPartition) LWindowFromString(in string) error {
	return f.lWindow.Deserialize([]byte(in))
}

func (f *SyntheticPartition) LWindowToString() string {
	result := f.lWindow.Serialize()
	return string(result)
}

//---------------------------------------------------------------------------------------------------------------------

func NewSyntheticPartition(syntheticPartitionNum int, overlapDuration time.Duration) *SyntheticPartition {
	return &SyntheticPartition{
		syntheticPartitionNum: syntheticPartitionNum,
		beforeListTime:        time.Time{},
		listDuration:          0,
		fullQueueToHandle:     ordered_multimap.NewOrderedMultimap(),
		lWindow:               l_window.NewLWindow(overlapDuration, 10),
	}
}
