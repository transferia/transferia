package pool

import (
	"github.com/transferria/transferria/library/go/core/xerrors"
)

var ErrPoolClosed = xerrors.New("pool is closed")

// TaskFn is used to process items (tasks)
// which are put into pool
type TaskFn func(interface{})

// Pool should be used to process set of homogeneous items (called tasks)
// in parallel
type Pool interface {
	// Add puts task into pool and waits until task processing is started.
	// Error is returned if task cannot be put into pool (ex. pool is closed), not if the processing itself has failed
	Add(task interface{}) error
	// Run makes pool ready to accept incoming tasks
	Run() error
	// Close stops accepting new tasks and waits until all tasks are processed and free internal pool resources
	Close() error
}
