package systemfields

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

type FieldType string

const (
	FieldTypeId         = "id"
	FieldTypeLsn        = "lsn"
	FieldTypeTxPosition = "tx_position"
	FieldTypeCommitTime = "commit_time"
	FieldTypeTxId       = "tx_id"
)

type fieldResolver struct {
	DataType ytschema.Type
	Value    func(abstract.ChangeItem) any
}

var fieldResolvers = map[FieldType]fieldResolver{
	FieldTypeId: {
		DataType: ytschema.TypeUint64,
		Value:    func(item abstract.ChangeItem) any { return item.ID },
	},
	FieldTypeLsn: {
		DataType: ytschema.TypeUint64,
		Value:    func(item abstract.ChangeItem) any { return item.LSN },
	},
	FieldTypeTxPosition: {
		DataType: ytschema.TypeInt64,
		Value:    func(item abstract.ChangeItem) any { return item.Counter },
	},
	FieldTypeCommitTime: {
		DataType: ytschema.TypeUint64,
		Value:    func(item abstract.ChangeItem) any { return item.CommitTime },
	},
	FieldTypeTxId: {
		DataType: ytschema.TypeString,
		Value:    func(item abstract.ChangeItem) any { return item.TxID },
	},
}

type SystemField struct {
	ColumnName string
	FieldType  FieldType
}

func NewSystemField(fieldType FieldType, colName string) (*SystemField, error) {
	if _, ok := fieldResolvers[fieldType]; !ok {
		return nil, xerrors.Errorf("field type %s is not known", fieldType)
	}
	return &SystemField{ColumnName: colName, FieldType: fieldType}, nil
}

func (f *SystemField) Type() ytschema.Type {
	return fieldResolvers[f.FieldType].DataType
}

func (f *SystemField) Value(item abstract.ChangeItem) any {
	return fieldResolvers[f.FieldType].Value(item)
}
