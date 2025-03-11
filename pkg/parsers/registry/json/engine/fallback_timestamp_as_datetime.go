package engine

import (
	"strings"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"github.com/transferia/transferia/pkg/parsers/generic"
	"go.ytsaurus.tech/yt/go/schema"
)

// isParsedItem - check if table is the result of generic-parser with opts.AddDedupeKeys columns
// that means we have 4 our system columns in the end of ColumnNames/TableSchema: _timestamp/_partition/_offset/_idx
// check if _timestamp is schema.TypeTimestamp - just in case.
// Actually type_system should guarantee it's always be called on old generic_parser, and always should be true
func isParsedItem(ci *abstract.ChangeItem) bool {
	if len(ci.TableSchema.Columns()) <= 4 {
		return false
	}
	timestampIndex := generic.TimestampIDX(ci.ColumnNames)
	if ci.TableSchema.Columns()[timestampIndex].ColumnName != generic.ColNameTimestamp ||
		ci.TableSchema.Columns()[timestampIndex].DataType != schema.TypeTimestamp.String() ||
		ci.ColumnNames[generic.PartitionIDX(ci.ColumnNames)] != generic.ColNamePartition ||
		ci.ColumnNames[generic.OffsetIDX(ci.ColumnNames)] != generic.ColNameOffset ||
		ci.ColumnNames[generic.ElemIDX(ci.ColumnNames)] != generic.ColNameIdx {
		return false
	}
	return true
}

// isUnparsedItem - check if table is the result of 'unparsed' case of generic-parser
// that means we have 6 our system columns: _timestamp/_partition/_offset/_idx/unparsed_row/reason
// check if _timestamp is schema.TypeTimestamp - just in case.
// Actually type_system should guarantee it's always be called on old generic_parser, and always should be true
func isUnparsedItem(ci *abstract.ChangeItem) bool {
	if len(ci.TableSchema.Columns()) != 6 {
		return false
	}
	if !strings.HasSuffix(ci.Table, "_unparsed") {
		return false
	}
	if ci.TableSchema.Columns()[0].ColumnName != generic.ColNameTimestamp ||
		ci.TableSchema.Columns()[0].DataType != schema.TypeTimestamp.String() ||
		ci.ColumnNames[1] != generic.ColNamePartition ||
		ci.ColumnNames[2] != generic.ColNameOffset ||
		ci.ColumnNames[3] != generic.ColNameIdx {
		return false
	}
	return true
}

func GenericParserTimestampFallback(ci *abstract.ChangeItem) (*abstract.ChangeItem, error) {
	switch ci.Kind {
	case abstract.InsertKind:
		if isParsedItem(ci) {
			ci.TableSchema.Columns()[generic.TimestampIDX(ci.ColumnNames)].DataType = schema.TypeDatetime.String()
			return ci, nil
		} else if isUnparsedItem(ci) {
			ci.TableSchema.Columns()[0].DataType = schema.TypeDatetime.String()
			return ci, nil
		}
		return ci, typesystem.FallbackDoesNotApplyErr
	default:
		return ci, typesystem.FallbackDoesNotApplyErr
	}
}
