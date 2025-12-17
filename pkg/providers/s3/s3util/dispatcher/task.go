package dispatcher

import (
	"fmt"
	"sort"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/file"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/synthetic_partition"
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

func (t *Task) syntheticPartitionByNum(syntheticPartitionNum int) (*synthetic_partition.SyntheticPartition, error) {
	result, ok := t.syntheticPartitionNumToSyntheticPartition[syntheticPartitionNum]
	if !ok {
		return nil, fmt.Errorf("no such syntheticPartition: %d", syntheticPartitionNum)
	}
	return result, nil
}

func (t *Task) BeforeListing() error {
	for _, currSyntheticPartition := range t.mySyntheticPartitions {
		err := currSyntheticPartition.BeforeListing()
		if err != nil {
			return xerrors.Errorf("unable to exec 'BeforeListing', err: %w", err)
		}
	}
	return nil
}

func (t *Task) AfterListing() {
	for _, currSyntheticPartition := range t.mySyntheticPartitions {
		currSyntheticPartition.AfterListing()
	}
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

func (t *Task) Commit(syntheticPartitionNum int, fileName string) error {
	currSyntheticPartition, err := t.syntheticPartitionByNum(syntheticPartitionNum)
	if err != nil {
		return xerrors.Errorf("failed to determine the syntheticPartition: %w", err)
	}
	err = currSyntheticPartition.Commit(fileName)
	if err != nil {
		return xerrors.Errorf("failed to add a new file: %w", err)
	}
	return nil
}

func (t *Task) FilesSorted() []file.File {
	result := make([]file.File, 0)
	for _, currSyntheticPartition := range t.mySyntheticPartitions {
		result = append(result, currSyntheticPartition.Files()...)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].LastModifiedNS < result[j].LastModifiedNS
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
	return currSyntheticPartition.LastCommittedStateFromString(state)
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
