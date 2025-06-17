//go:build !disable_s3_provider

package dispatcher

import (
	"sync"

	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/providers/s3/source/object_fetcher/poller/dispatcher/file"
)

type Dispatcher struct {
	dispatcherImmutablePart *DispatcherImmutablePart
	myTask                  *Task
	commitMutex             sync.Mutex
}

func (d *Dispatcher) IsMyFileName(fileName string) bool {
	return d.dispatcherImmutablePart.IsMyFileName(fileName)
}

func (d *Dispatcher) DetermineSyntheticPartitionNum(fileName string) int {
	return d.dispatcherImmutablePart.DetermineSyntheticPartitionNum(fileName)
}

func (d *Dispatcher) IsMySyntheticPartitionNum(syntheticPartitionNum int) bool {
	return d.myTask.Contains(syntheticPartitionNum)
}

func (d *Dispatcher) MySyntheticPartitionNums() []int {
	return d.myTask.MySyntheticPartitionNums()
}

func (d *Dispatcher) ResetBeforeListing() error {
	return d.myTask.ResetBeforeListing()
}

func (d *Dispatcher) AddIfNew(file *file.File) (bool, error) {
	syntheticPartitionNum := d.dispatcherImmutablePart.DetermineSyntheticPartitionNum(file.FileName)
	return d.myTask.AddIfNew(syntheticPartitionNum, file)
}

func (d *Dispatcher) ExtractSortedFileNames() ([]string, error) {
	filesSorted := d.myTask.FilesSorted()
	result := make([]string, 0, len(filesSorted))
	for _, currFile := range filesSorted {
		result = append(result, currFile.FileName)
	}
	return result, nil
}

func (d *Dispatcher) Commit(fileName string) (bool, error) {
	d.commitMutex.Lock()
	defer d.commitMutex.Unlock()

	syntheticPartitionNum := d.dispatcherImmutablePart.DetermineSyntheticPartitionNum(fileName)
	return d.myTask.Commit(syntheticPartitionNum, fileName)
}

func (d *Dispatcher) CommitAll() error {
	return d.myTask.CommitAll()
}

func (d *Dispatcher) InitSyntheticPartitionNumByState(syntheticPartitionNum int, state string) error {
	return d.myTask.SetState(syntheticPartitionNum, state)
}

func (d *Dispatcher) SerializeState() map[string]*coordinator.TransferStateData {
	d.commitMutex.Lock()
	defer d.commitMutex.Unlock()

	states := d.myTask.SyntheticPartitionToState()

	result := make(map[string]*coordinator.TransferStateData)
	for k, v := range states {
		//nolint:exhaustivestruct
		result[k] = &coordinator.TransferStateData{
			Generic: v,
		}
	}
	return result
}

func NewDispatcher(numberOfSyntheticPartitions int, myWorkerProperties *WorkerProperties) *Dispatcher {
	dispatcherStatelessPart := NewDispatcherImmutablePart(numberOfSyntheticPartitions, myWorkerProperties)
	myTask := dispatcherStatelessPart.generateMySyntheticPartitions()

	return &Dispatcher{
		dispatcherImmutablePart: dispatcherStatelessPart,
		myTask:                  myTask,
		commitMutex:             sync.Mutex{},
	}
}
