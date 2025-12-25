package worker_pool

import (
	"sync"
)

type defaultPool struct {
	taskCh   chan interface{}
	workerWG sync.WaitGroup
	capacity uint64
	taskFn   TaskFn
	closeMu  sync.RWMutex
	isClosed bool
}

func (t *defaultPool) Add(task interface{}) error {
	t.closeMu.RLock()
	defer t.closeMu.RUnlock()
	if t.isClosed {
		return ErrPoolClosed
	}
	t.taskCh <- task
	return nil
}

func (t *defaultPool) Run() error {
	for i := uint64(0); i < t.capacity; i++ {
		t.workerWG.Add(1)
		go t.worker()
	}
	return nil
}

func (t *defaultPool) Close() error {
	t.closeMu.Lock()
	defer t.closeMu.Unlock()
	close(t.taskCh)
	t.isClosed = true
	t.workerWG.Wait()
	return nil
}

func (t *defaultPool) worker() {
	defer t.workerWG.Done()

	for task := range t.taskCh {
		t.taskFn(task)
	}
}

// NewDefaultWorkerPool creates simple task pool.
// taskFn is called to process each task. Capacity sets maximum number of tasks to be processed simultaneously
func NewDefaultWorkerPool(taskFn TaskFn, capacity uint64) WorkerPool {
	return &defaultPool{
		taskCh:   make(chan interface{}),
		workerWG: sync.WaitGroup{},
		capacity: capacity,
		taskFn:   taskFn,
		closeMu:  sync.RWMutex{},
		isClosed: false,
	}
}
