//go:build !disable_s3_provider

package synthetic_partition

import (
	"fmt"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/s3/source/object_fetcher/poller/dispatcher/file"
	"github.com/transferia/transferia/pkg/providers/s3/source/object_fetcher/poller/synthetic_partition/ordered_multimap"
)

type SyntheticPartition struct {
	// parameter
	syntheticPartitionNum int

	//------------------------------------------
	// state
	last_committed_state *lastCommittedState
	queueToHandle        *ordered_multimap.OrderedMultiMapWrapped
	committed            *ordered_multimap.OrderedMultiMapWrapped
}

func (f *SyntheticPartition) SyntheticPartitionNum() int {
	return f.syntheticPartitionNum
}

func (f *SyntheticPartition) SyntheticPartitionStr() string {
	return fmt.Sprintf("%d", f.syntheticPartitionNum)
}

//---------------------------------------------------------------------------------------------------------------------
// stateful part

func (f *SyntheticPartition) ResetBeforeListing() error {
	if f.queueToHandle.Size() != 0 {
		return xerrors.Errorf("contract is broken - on 'listing' stage, queueToHandle should be empty")
	}
	f.committed = ordered_multimap.NewOrderedMultiMapWrapped()
	return nil
}

func (f *SyntheticPartition) Files() []*file.File {
	result := make([]*file.File, 0, f.queueToHandle.Size()*2)
	values := f.queueToHandle.Values()
	for _, v := range values {
		result = append(result, v...)
	}
	return result
}

func (f *SyntheticPartition) InitByLastCommittedState(last_committed_state *lastCommittedState) {
	f.last_committed_state = last_committed_state
}

func (f *SyntheticPartition) AddIfNew(newFile *file.File) (bool, error) {
	if f.last_committed_state.IsNew(newFile) {
		// ADD
		ns := newFile.LastModified.UnixNano()
		err := f.queueToHandle.Add(ns, newFile)
		if err != nil {
			return false, xerrors.Errorf("unable to add file into queueToHandle, err: %w", err)
		}
		return true, nil
	}

	// SKIP, we already committed it
	return false, nil
}

func (f *SyntheticPartition) Commit(fileName string) (bool, error) {
	lastCommittedStateChange := false

	// find 'File' object
	currFile, err := f.queueToHandle.FileByFileName(fileName)
	if err != nil {
		return false, xerrors.Errorf("unable to get file, err: %w", err)
	}
	currFileNS := currFile.LastModified.UnixNano()

	// validate
	minNsToCommit, _, err := f.queueToHandle.FirstPair()
	if err != nil {
		return false, xerrors.Errorf("unable to get first pair, err: %w", err)
	}

	// remove 'File' from queueToHandle
	err = f.queueToHandle.DelOne(currFileNS, fileName)
	if err != nil {
		return false, xerrors.Errorf("unable to remove file, err: %w", err)
	}

	// commit
	err = f.committed.Add(currFileNS, currFile)
	if err != nil {
		return false, xerrors.Errorf("unable to add file into committed, err: %w", err)
	}

	// calculate new 'last_committed_state'
	if minNsToCommit == currFileNS { // it means, we commit minimum NS from 'queueToHandle' - we need calculate new 'last_committed_state'
		lastCommittedStateChange = true

		if f.queueToHandle.Empty() {
			_, v, err := f.committed.LastPair()
			if err != nil {
				return false, xerrors.Errorf("unable to get last pair, err: %w", err)
			}
			f.last_committed_state.SetNew(v)
		} else {
			nextMinNsToCommit, _, err := f.queueToHandle.FirstPair()
			if err != nil {
				return false, xerrors.Errorf("unable to get first pair, err: %w", err)
			}
			bestCommittedNS, err := f.committed.FindClosestKey(nextMinNsToCommit)
			if err != nil {
				return false, xerrors.Errorf("unable to find closest key, err: %w", err)
			}
			if bestCommittedNS < currFileNS || bestCommittedNS > nextMinNsToCommit {
				return false, xerrors.Errorf("some invariant is broken")
			}

			v, err := f.committed.Get(bestCommittedNS)
			if err != nil {
				return false, xerrors.Errorf("some invariant is broken, err: %w", err)
			}
			f.last_committed_state.SetNew(v)
		}
	}

	return lastCommittedStateChange, nil
}

func (f *SyntheticPartition) CommitAll() error {
	if f.queueToHandle.Empty() {
		last_committed_state, err := newLastCommittedState(0, nil)
		if err != nil {
			return xerrors.Errorf("could not create last committed state: %w", err)
		}
		f.last_committed_state = last_committed_state
		return nil
	} else {
		ns, files, _ := f.queueToHandle.LastPair()
		newState, err := f.last_committed_state.CalculateNewLastCommittedState(ns, files)
		if err != nil {
			return xerrors.Errorf("could not calculate new last committed state, err: %w", err)
		}
		f.last_committed_state = newState
		f.queueToHandle.Reset()
		return nil
	}
}

func (f *SyntheticPartition) LastCommittedStateFromString(in string) {
	f.last_committed_state.FromString(in)
}

func (f *SyntheticPartition) LastCommittedStateToString() string {
	return f.last_committed_state.ToString()
}

//---------------------------------------------------------------------------------------------------------------------

func NewSyntheticPartition(syntheticPartitionNum int) *SyntheticPartition {
	currLastCommittedState, _ := newLastCommittedState(0, nil)
	return &SyntheticPartition{
		syntheticPartitionNum: syntheticPartitionNum,
		last_committed_state:  currLastCommittedState,
		queueToHandle:         ordered_multimap.NewOrderedMultiMapWrapped(),
		committed:             ordered_multimap.NewOrderedMultiMapWrapped(),
	}
}
