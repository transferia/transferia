//go:build !disable_s3_provider

package dispatcher

import (
	"fmt"
	"sort"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/s3/source/object_fetcher/poller/dispatcher/file"
	"github.com/transferia/transferia/pkg/providers/s3/source/object_fetcher/poller/synthetic_partition"
	"golang.org/x/exp/maps"
)

type Task struct {
	mySyntheticPartitions                     []*synthetic_partition.SyntheticPartition
	syntheticPartitionNumToSyntheticPartition map[int]*synthetic_partition.SyntheticPartition
}

func (t *Task) MySyntheticPartitionNums() []int {
	result := maps.Keys(t.syntheticPartitionNumToSyntheticPartition)
	sort.Ints(result)
	return result
}

func (t *Task) Contains(syntheticPartitionNum int) bool {
	_, ok := t.syntheticPartitionNumToSyntheticPartition[syntheticPartitionNum]
	return ok
}

func (t *Task) syntheticPartitionByNum(syntheticPartitionNum int) (*synthetic_partition.SyntheticPartition, error) {
	result, ok := t.syntheticPartitionNumToSyntheticPartition[syntheticPartitionNum]
	if !ok {
		return nil, fmt.Errorf("no such syntheticPartition: %d", syntheticPartitionNum)
	}
	return result, nil
}

func (t *Task) ResetBeforeListing() error {
	for _, currSyntheticPartition := range t.mySyntheticPartitions {
		err := currSyntheticPartition.ResetBeforeListing()
		if err != nil {
			return xerrors.Errorf("unable to reset, err: %w", err)
		}
	}
	return nil
}

func (t *Task) AddIfNew(syntheticPartitionNum int, file *file.File) (bool, error) {
	currSyntheticPartition, err := t.syntheticPartitionByNum(syntheticPartitionNum)
	if err != nil {
		return false, xerrors.Errorf("failed to determine the syntheticPartition: %w", err)
	}
	added, err := currSyntheticPartition.AddIfNew(file)
	if err != nil {
		return false, xerrors.Errorf("failed to add a new file: %w", err)
	}
	return added, nil
}

func (t *Task) Commit(syntheticPartitionNum int, fileName string) (bool, error) {
	currSyntheticPartition, err := t.syntheticPartitionByNum(syntheticPartitionNum)
	if err != nil {
		return false, xerrors.Errorf("failed to determine the syntheticPartition: %w", err)
	}
	lastCommittedStateChange, err := currSyntheticPartition.Commit(fileName)
	if err != nil {
		return false, xerrors.Errorf("failed to add a new file: %w", err)
	}
	return lastCommittedStateChange, nil
}

func (t *Task) FilesSorted() []*file.File {
	result := make([]*file.File, 0)
	for _, currSyntheticPartition := range t.mySyntheticPartitions {
		result = append(result, currSyntheticPartition.Files()...)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].LastModified.UnixNano() < result[j].LastModified.UnixNano()
	})
	return result
}

func (t *Task) SyntheticPartitionToState() map[string]string {
	result := make(map[string]string)
	for _, currSyntheticPartition := range t.mySyntheticPartitions {
		result[currSyntheticPartition.SyntheticPartitionStr()] = currSyntheticPartition.LastCommittedStateToString()
	}
	return result
}

func (t *Task) SetState(syntheticPartitionNum int, state string) error {
	currSyntheticPartition, ok := t.syntheticPartitionNumToSyntheticPartition[syntheticPartitionNum]
	if !ok {
		return fmt.Errorf("no such syntheticPartition: %d", syntheticPartitionNum)
	}
	currSyntheticPartition.LastCommittedStateFromString(state)
	return nil
}

func (t *Task) CommitAll() error {
	for _, currSyntheticPartition := range t.mySyntheticPartitions {
		err := currSyntheticPartition.CommitAll()
		if err != nil {
			fmt.Printf("Error committing syntheticPartition: %s", err.Error())
		}
	}
	return nil
}

func NewTask(in []*synthetic_partition.SyntheticPartition) *Task {
	syntheticPartitionNumToSyntheticPartition := make(map[int]*synthetic_partition.SyntheticPartition)
	for _, currSyntheticPartition := range in {
		syntheticPartitionNumToSyntheticPartition[currSyntheticPartition.SyntheticPartitionNum()] = currSyntheticPartition
	}
	return &Task{
		mySyntheticPartitions:                     in,
		syntheticPartitionNumToSyntheticPartition: syntheticPartitionNumToSyntheticPartition,
	}
}
