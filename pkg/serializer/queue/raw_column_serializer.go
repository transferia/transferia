package queue

import (
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
)

type RawColumnSerializer struct {
	columnName string
	logger     log.Logger
}

func NewRawColumnSerializer(columnName string, logger log.Logger) *RawColumnSerializer {
	return &RawColumnSerializer{columnName: columnName, logger: logger}
}

func (s *RawColumnSerializer) Serialize(input []abstract.ChangeItem) (map[abstract.TablePartID][]SerializedMessage, error) {
	if len(input) == 0 {
		return nil, nil
	}

	skippedCount := 0
	var lastError error
	idToGroup := make(map[abstract.TablePartID][]SerializedMessage)
	for _, changeItem := range input {
		columnValue, err := s.extractColumnValue(&changeItem)
		if err != nil {
			lastError = err
			skippedCount++
			continue
		}
		partID := changeItem.TablePartID()
		idToGroup[partID] = append(idToGroup[partID], columnValue)
	}
	if lastError != nil {
		s.logger.Warn("some rows were skipped", log.NamedError("last_error", lastError), log.Int("skipped_count", skippedCount))
	}
	return idToGroup, nil
}

func (s *RawColumnSerializer) extractColumnValue(changeItem *abstract.ChangeItem) (SerializedMessage, error) {
	for i, columnName := range changeItem.ColumnNames {
		if columnName == s.columnName {
			payload, err := extractBytesValue(changeItem, i)
			return SerializedMessage{Key: nil, Value: payload}, err
		}
	}
	return SerializedMessage{Key: nil, Value: nil}, xerrors.Errorf("column %q not found", s.columnName)
}

func extractBytesValue(changeItem *abstract.ChangeItem, columnIndex int) ([]byte, error) {
	columnName := abstract.ColumnName(changeItem.ColumnNames[columnIndex])
	colSchema, ok := changeItem.TableSchema.FastColumns()[columnName]
	if !ok {
		return nil, xerrors.Errorf("table schema does not contain column %q", columnName)
	}
	if colSchema.DataType != string(schema.TypeString) && colSchema.DataType != string(schema.TypeBytes) {
		return nil, xerrors.Errorf("expected type of the column %q to be %s or %s but got %s", columnName, schema.TypeString, schema.TypeBytes, colSchema.DataType)
	}

	// Possible types for TypeString and TypeBytes are defined here:
	// https://github.com/transferria/transferria/arcadia/transfer_manager/go/pkg/abstract/typesystem/values/type_checkers.go?rev=11543367#L52-53
	switch columnValue := changeItem.ColumnValues[columnIndex].(type) {
	case string:
		return []byte(columnValue), nil
	case []byte:
		return columnValue, nil
	default:
		return nil, xerrors.Errorf("unexpected column value type: %T", columnValue)
	}
}
