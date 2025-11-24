package clickhouse

import (
	"fmt"

	"github.com/doublecloud/transfer/pkg/abstract"
)

type InsertBlockStore interface {
	Get(id abstract.TablePartID) (*InsertBlock, InsertBlockStatus, error)
	Set(id abstract.TablePartID, block *InsertBlock, status InsertBlockStatus) error
}

type InsertBlockStatus string

const (
	InsertBlockStatusEmpty  = InsertBlockStatus("")
	InsertBlockStatusBefore = InsertBlockStatus("before")
	InsertBlockStatusAfter  = InsertBlockStatus("after")
)

var (
	_ fmt.Stringer = (*InsertBlock)(nil)
)

type InsertBlock struct {
	min, max uint64
}

func (b InsertBlock) String() string {
	return fmt.Sprintf("%v->%v", b.min, b.max)
}
