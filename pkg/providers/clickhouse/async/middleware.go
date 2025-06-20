//go:build !disable_clickhouse_provider

package async

import (
	"io"
	"sync"

	"github.com/transferia/transferia/pkg/abstract"
)

type MiddlewareApplier interface {
	io.Closer
	Apply([]abstract.ChangeItem) ([]abstract.ChangeItem, error)
}

type mwApplier struct {
	lock   sync.Mutex
	rows   []abstract.ChangeItem
	mwSink abstract.Sinker
}

func (d *mwApplier) Close() error {
	return nil
}

func (d *mwApplier) Push(items []abstract.ChangeItem) error {
	d.rows = items
	return nil
}

func (d *mwApplier) Apply(items []abstract.ChangeItem) ([]abstract.ChangeItem, error) {
	if d.mwSink == nil {
		return items, nil
	}
	d.lock.Lock()
	defer d.lock.Unlock()
	if err := d.mwSink.Push(items); err != nil {
		return nil, err
	}
	return d.rows, nil
}

func NewMiddlewareApplier(mw abstract.Middleware) MiddlewareApplier {
	res := new(mwApplier)
	if mw != nil {
		res.mwSink = mw(res)
	}
	return res
}
