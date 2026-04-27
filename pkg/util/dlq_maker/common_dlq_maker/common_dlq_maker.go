package common_dlq_maker

import (
	"encoding/json"
	"fmt"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/util/jsonx"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

const (
	ColNameYqlTransformerErrorMessage = "yql_transformer_error_message"
	// ColNameYqlTransformerRowID is a synthetic field appended to the JSON sent to yql_serverd; it is not
	// part of the table schema. When used with matchMalformedByRowID, the sidecar includes it in malformed
	// row JSON so the client can map back to the original change item by index.
	ColNameYqlTransformerRowID = "__dt_yql_transformer_row_id"
)

func buildPkeyString(changeItem abstract.ChangeItem) string {
	return changeItem.CurrentKeysString(changeItem.MakeMapKeys())
}

func coalesceRowID(v any) (int, error) {
	switch x := v.(type) {
	case int:
		return x, nil
	case int32:
		return int(x), nil
	case int64:
		return int(x), nil
	case float64:
		return int(x), nil
	case json.Number:
		i, err := x.Int64()
		if err != nil {
			return 0, err
		}
		return int(i), nil
	default:
		return 0, fmt.Errorf("unexpected type for row_id: %T", v)
	}
}

func buildDLQChangeItemsByPkey(
	inChangeItems []abstract.ChangeItem,
	malformedRows []MalformedRow,
	suffix string,
	newSchema []abstract.ColSchema,
) ([]abstract.ChangeItem, error) {
	rowToInChangeItem := make(map[string]*abstract.ChangeItem)
	for _, currentRow := range inChangeItems {
		rowToInChangeItem[buildPkeyString(currentRow)] = &currentRow
	}
	if len(rowToInChangeItem) != len(inChangeItems) {
		return nil, xerrors.Errorf("rowToInChangeItem and inChangeItems are not the same length, len(rowToInChangeItem): %d, len(inChangeItems): %d", len(rowToInChangeItem), len(inChangeItems))
	}

	result := make([]abstract.ChangeItem, 0, len(malformedRows))
	for _, malformedRow := range malformedRows {
		var unmarshalled map[string]any
		err := jsonx.Unmarshal(malformedRow.Row, &unmarshalled)
		if err != nil {
			return nil, abstract.NewFatalError(xerrors.Errorf("malformed row %s: %w", malformedRow, err))
		}

		var currentChangeItem abstract.ChangeItem
		currentChangeItem.TableSchema = inChangeItems[0].TableSchema
		for k, v := range unmarshalled {
			currentChangeItem.ColumnNames = append(currentChangeItem.ColumnNames, k)
			currentChangeItem.ColumnValues = append(currentChangeItem.ColumnValues, v)
		}
		key := buildPkeyString(currentChangeItem)

		currChangeItem, ok := rowToInChangeItem[key]
		if !ok {
			return nil, abstract.NewFatalError(xerrors.Errorf("row %s not found", key))
		}
		newChangeItem := *currChangeItem
		newChangeItem.Table = newChangeItem.Table + suffix
		newChangeItem.TableSchema = changeitem.NewTableSchema(newSchema)
		newChangeItem.ColumnNames = append(newChangeItem.ColumnNames, ColNameYqlTransformerErrorMessage)
		newChangeItem.ColumnValues = append(newChangeItem.ColumnValues, malformedRow.Error)
		result = append(result, newChangeItem)
	}
	return result, nil
}

func buildDLQChangeItemsByRowID(
	inChangeItems []abstract.ChangeItem,
	malformedRows []MalformedRow,
	suffix string,
	newSchema []abstract.ColSchema,
) ([]abstract.ChangeItem, error) {
	result := make([]abstract.ChangeItem, 0, len(malformedRows))
	for _, malformedRow := range malformedRows {
		var obj map[string]any
		if err := jsonx.Unmarshal(malformedRow.Row, &obj); err != nil {
			return nil, abstract.NewFatalError(xerrors.Errorf("malformed row %s: %w", malformedRow, err))
		}
		raw, ok := obj[ColNameYqlTransformerRowID]
		if !ok {
			return nil, abstract.NewFatalError(xerrors.Errorf("malformed row has no %s: %s", ColNameYqlTransformerRowID, string(malformedRow.Row)))
		}
		rowID, err := coalesceRowID(raw)
		if err != nil {
			return nil, abstract.NewFatalError(xerrors.Errorf("invalid %s in malformed row: %w", ColNameYqlTransformerRowID, err))
		}
		if rowID < 0 || rowID >= len(inChangeItems) {
			return nil, abstract.NewFatalError(xerrors.Errorf("row_id %d out of range (len=%d)", rowID, len(inChangeItems)))
		}
		newChangeItem := inChangeItems[rowID]
		newChangeItem.Table = newChangeItem.Table + suffix
		newChangeItem.TableSchema = changeitem.NewTableSchema(newSchema)
		newChangeItem.ColumnNames = append(newChangeItem.ColumnNames, ColNameYqlTransformerErrorMessage)
		newChangeItem.ColumnValues = append(newChangeItem.ColumnValues, malformedRow.Error)
		result = append(result, newChangeItem)
	}
	return result, nil
}

// BuildDLQChangeItems appends a DLQ error column and routes malformed YQL rows back to the corresponding
// input change items. When matchMalformedByRowID is true, the mapping uses ColNameYqlTransformerRowID from
// the malformed row JSON (0-based index into inChangeItems). When false, the prior primary-key string match
// is used.
func BuildDLQChangeItems(inChangeItems []abstract.ChangeItem, malformedRows []MalformedRow, suffix string, matchMalformedByRowID bool) ([]abstract.ChangeItem, error) {
	if len(malformedRows) == 0 || len(inChangeItems) == 0 {
		return nil, nil
	}

	masterChangeItem := inChangeItems[0]
	if _, ok := masterChangeItem.AsMap()[ColNameYqlTransformerErrorMessage]; ok {
		return nil, abstract.NewFatalError(xerrors.Errorf("table %s already has column %s", masterChangeItem.Table, ColNameYqlTransformerErrorMessage))
	}
	if matchMalformedByRowID {
		for _, c := range masterChangeItem.TableSchema.Columns() {
			if c.ColumnName == ColNameYqlTransformerRowID {
				return nil, abstract.NewFatalError(xerrors.Errorf("table %s must not have column name %s (reserved for yql sidecar)", masterChangeItem.Table, ColNameYqlTransformerRowID))
			}
		}
	}

	newSchema := masterChangeItem.TableSchema.Columns()
	newSchema = append(newSchema, changeitem.NewColSchema(ColNameYqlTransformerErrorMessage, ytschema.TypeString, false))

	if matchMalformedByRowID {
		result, err := buildDLQChangeItemsByRowID(inChangeItems, malformedRows, suffix, newSchema)
		if err != nil {
			return nil, xerrors.Errorf("failed to build DLQ change items (by row_id), err: %w", err)
		}
		return result, nil
	}
	result, err := buildDLQChangeItemsByPkey(inChangeItems, malformedRows, suffix, newSchema)
	if err != nil {
		return nil, xerrors.Errorf("failed to build DLQ change items (by pkey), err: %w", err)
	}
	return result, nil
}
