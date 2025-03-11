package common

import "github.com/transferria/transferria/pkg/abstract"

type ChangeItemCanon struct {
	ChangeItem     *abstract.ChangeItem
	DebeziumEvents []KeyValue
}
