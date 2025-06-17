//go:build !disable_clickhouse_provider

package schema

import (
	"encoding/binary"

	"github.com/transferia/transferia/pkg/base"
)

type DDLBatch struct {
	DDLs []TableDDL
	iter int
}

func (b *DDLBatch) Count() int {
	return len(b.DDLs)
}

func (b *DDLBatch) Size() int {
	return binary.Size(b.DDLs)
}

func (b *DDLBatch) Next() bool {
	b.iter++
	return len(b.DDLs) < b.iter
}

func (b *DDLBatch) Event() (base.Event, error) {
	return b.DDLs[b.iter], nil
}

func NewDDLBatch(ddls []TableDDL) *DDLBatch {
	return &DDLBatch{
		DDLs: ddls,
		iter: -1,
	}
}
