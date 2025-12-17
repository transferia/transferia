package dispatcher

import (
	"sync"
	"time"

	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/effective_worker_num"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/file"
	"github.com/transferia/transferia/pkg/providers/s3/s3util/lr_window/r_window"
)

type Dispatcher struct {
	rWindow                 *r_window.RWindow
	dispatcherImmutablePart *DispatcherImmutablePart
	myTask                  *Task
	commitMutex             sync.Mutex
}

func (d *Dispatcher) IsMyFileName(fileName string) bool {
	return d.dispatcherImmutablePart.IsMyFileName(fileName)
}

func (d *Dispatcher) IsOutOfRWindow(inFile *file.File) bool {
	return d.rWindow.IsOutOfRWindow(inFile)
}

func (d *Dispatcher) DetermineSyntheticPartitionNum(fileName string) int {
	return d.dispatcherImmutablePart.DetermineSyntheticPartitionNum(fileName)
}

func (d *Dispatcher) IsMySyntheticPartitionNum(syntheticPartitionNum int) bool {
	return d.dispatcherImmutablePart.Contains(syntheticPartitionNum)
}

func (d *Dispatcher) MySyntheticPartitionNums() []int {
	return d.myTask.MySyntheticPartitionNums()
}

func (d *Dispatcher) BeforeListing() error {
	return d.myTask.BeforeListing()
}

func (d *Dispatcher) AfterListing() {
	d.myTask.AfterListing()
}

func (d *Dispatcher) AddIfNew(file *file.File) (bool, error) {
	syntheticPartitionNum := d.dispatcherImmutablePart.DetermineSyntheticPartitionNum(file.FileName)
	return d.myTask.AddIfNew(syntheticPartitionNum, file)
}

func (d *Dispatcher) ExtractSortedFileEntries() []file.File {
	return d.myTask.FilesSorted()
}

func (d *Dispatcher) Commit(fileName string) error {
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

func NewDispatcher(
	rWindow *r_window.RWindow,
	numberOfSyntheticPartitions int,
	inEffectiveWorkerNum *effective_worker_num.EffectiveWorkerNum,
	overlapDuration time.Duration,
) *Dispatcher {
	dispatcherStatelessPart := NewDispatcherImmutablePart(numberOfSyntheticPartitions, inEffectiveWorkerNum, overlapDuration)
	myTask := dispatcherStatelessPart.generateMySyntheticPartitions()

	return &Dispatcher{
		rWindow:                 rWindow,
		dispatcherImmutablePart: dispatcherStatelessPart,
		myTask:                  myTask,
		commitMutex:             sync.Mutex{},
	}
}
