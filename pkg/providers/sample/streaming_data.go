package sample

import (
	"github.com/transferia/transferia/pkg/abstract"
)

type StreamingData interface {
	TableName() abstract.TableID
	ToChangeItem(offset int64) abstract.ChangeItem
}
