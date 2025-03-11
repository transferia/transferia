package sample

import (
	"github.com/transferria/transferria/pkg/abstract"
)

type StreamingData interface {
	TableName() abstract.TableID
	ToChangeItem(offset int64) abstract.ChangeItem
}
