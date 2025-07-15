package ydb

import (
	"encoding/base64"
	"encoding/json"
	"sort"
	"strconv"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"golang.org/x/exp/maps"
)

func makeVal(originalType string, val interface{}) (interface{}, error) {
	if val == nil {
		return nil, nil
	}

	switch originalType {
	case "ydb:Bool":
		return val.(bool), nil
	case "ydb:Int32", "ydb:Int64":
		v := val.(json.Number)
		result, err := v.Int64()
		if err != nil {
			return nil, xerrors.Errorf("unable to convert json.Number into int64, val:%s, err:%w", v.String(), err)
		}
		return result, nil
	case "ydb:Int8":
		v := val.(json.Number)
		result, err := v.Int64()
		if err != nil {
			return nil, xerrors.Errorf("unable to convert json.Number into int64, val:%s, err:%w", v.String(), err)
		}
		return int8(result), nil
	case "ydb:Int16":
		v := val.(json.Number)
		result, err := v.Int64()
		if err != nil {
			return nil, xerrors.Errorf("unable to convert json.Number into int64, val:%s, err:%w", v.String(), err)
		}
		return int16(result), nil
	case "ydb:Uint8":
		v := val.(json.Number)
		result, err := v.Int64()
		if err != nil {
			return nil, xerrors.Errorf("unable to convert json.Number into int64, val:%s, err:%w", v.String(), err)
		}
		return uint8(result), nil
	case "ydb:Uint16":
		v := val.(json.Number)
		result, err := v.Int64()
		if err != nil {
			return nil, xerrors.Errorf("unable to convert json.Number into int64, val:%s, err:%w", v.String(), err)
		}
		return uint16(result), nil
	case "ydb:Uint32":
		v := val.(json.Number)
		result, err := v.Int64()
		if err != nil {
			return nil, xerrors.Errorf("unable to convert json.Number into int64, val:%s, err:%w", v.String(), err)
		}
		return uint32(result), nil
	case "ydb:Uint64":
		uint64str := val.(json.Number).String()
		result, err := strconv.ParseUint(uint64str, 10, 64)
		if err != nil {
			return nil, xerrors.Errorf("unable to convert json.Number into uint64, val:%s, err:%w", uint64str, err)
		}
		return result, nil
	case "ydb:Float":
		v := val.(json.Number)
		result, err := v.Float64()
		if err != nil {
			return nil, xerrors.Errorf("unable to convert json.Number into Float64, val:%s, err:%w", v.String(), err)
		}
		return float32(result), nil
	case "ydb:Double":
		v := val.(json.Number)
		result, err := v.Float64()
		if err != nil {
			return nil, xerrors.Errorf("unable to convert json.Number into Float64, val:%s, err:%w", v.String(), err)
		}
		return result, nil
	case "ydb:Decimal":
		return string(addZerosToDecimal([]byte(val.(string)))), nil
	case "ydb:DyNumber":
		return val.(string), nil
	case "ydb:String":
		valStr := val.(string)
		processedData, err := base64.StdEncoding.DecodeString(valStr)
		if err != nil {
			return nil, xerrors.Errorf("unable to decode base64, val:%s, err:%w", valStr, err)
		}
		return processedData, nil
	case "ydb:Utf8":
		return val.(string), nil
	case "ydb:Json":
		return val, nil
	case "ydb:JsonDocument":
		return val, nil
	case "ydb:Date":
		// Date, precision to the day
		result, err := time.Parse("2006-01-02", val.(string)[0:10])
		if err != nil {
			return nil, xerrors.Errorf("unable to parse time, val:%s, err:%w", val.(string), err)
		}
		return result, nil
	case "ydb:Datetime":
		// Date/time, precision to the second
		result, err := time.Parse("2006-01-02T15:04:05", val.(string)[0:19])
		if err != nil {
			return nil, xerrors.Errorf("unable to parse time, val:%s, err:%w", val.(string), err)
		}
		return result, nil
	case "ydb:Timestamp":
		// Date/time, precision to the microsecond
		result, err := time.Parse("2006-01-02T15:04:05.000000", val.(string)[0:26])
		if err != nil {
			return nil, xerrors.Errorf("unable to parse time, val:%s, err:%w", val.(string), err)
		}
		return result, nil
	case "ydb:Interval":
		// Time interval (signed), precision to microseconds
		v := val.(json.Number)
		result, err := v.Int64()
		if err != nil {
			return nil, xerrors.Errorf("unable to convert json.Number into int64, val:%s, err:%w", v.String(), err)
		}
		return time.Duration(result) * time.Microsecond, nil
	case "ydb:Uuid":
		v := val.(string)
		return v, nil
	default:
		return nil, xerrors.Errorf("unknown originalType: %s", originalType)
	}
}

func addZerosToDecimal(value []byte) []byte {
	dotIndex := 0
	if len(value) > 10 {
		dotIndex = len(value) - 10
	}
	for ; dotIndex < len(value); dotIndex++ {
		if value[dotIndex] == '.' {
			break
		}
	}
	outValue := make([]byte, dotIndex+10)
	copy(outValue, value)
	for i := len(value); i < len(outValue); i++ {
		outValue[i] = '0'
	}
	outValue[dotIndex] = '.'
	return outValue
}

func makeColNameColVal(schema abstract.TableColumns, colNameToIndex map[string]int, colName string, colVal interface{}) (interface{}, error) {
	colIdx, ok := colNameToIndex[colName]
	if !ok {
		return nil, xerrors.Errorf("unable to find columnName, colName: %s", colName)
	}
	val, err := makeVal(schema[colIdx].OriginalType, colVal)
	if err != nil {
		return nil, xerrors.Errorf("unable to make value, err: %w", err)
	}
	return val, nil
}

func makeUpdateChangeItem(
	tablePath string,
	schema *abstract.TableSchema,
	event *cdcEvent,
	writeTime time.Time,
	offset int64,
	partitionID int64,
	msgSize uint64,
	fillDefaults bool,
) (*abstract.ChangeItem, error) {
	colNameToIndex := abstract.MakeMapColNameToIndex(schema.Columns())
	keyColumnNames := abstract.KeyNames(schema.Columns())
	result := &abstract.ChangeItem{
		ID:           0,
		LSN:          uint64(offset),
		CommitTime:   uint64(writeTime.UTC().UnixNano()),
		Counter:      0,
		Kind:         abstract.UpdateKind,
		Schema:       "",
		Table:        tablePath,
		PartID:       strconv.Itoa(int(partitionID)),
		ColumnNames:  make([]string, 0, len(event.Key)+len(event.Update)),
		ColumnValues: make([]interface{}, 0, len(event.Key)+len(event.Update)),
		TableSchema:  schema,
		OldKeys: abstract.OldKeysType{
			KeyNames:  make([]string, 0, len(event.Key)+len(event.OldImage)),
			KeyTypes:  make([]string, 0, len(event.Key)+len(event.OldImage)),
			KeyValues: make([]interface{}, 0, len(event.Key)+len(event.OldImage)),
		},
		TxID:  "",
		Query: "",
		Size:  abstract.RawEventSize(msgSize),
	}
	index := 0
	for _, keyVal := range event.Key {
		if index >= len(keyColumnNames) {
			return nil, xerrors.Errorf("unable to handle changefeed event - wrong amount of pkey columns, index: %d, len(keyColumnNames): %d, keyColumnNames: %v", index, len(keyColumnNames), keyColumnNames)
		}
		currColName := keyColumnNames[index]
		val, err := makeColNameColVal(schema.Columns(), colNameToIndex, currColName, keyVal)
		if err != nil {
			return nil, xerrors.Errorf("unable to make value, keyName: %s, err: %w", currColName, err)
		}
		result.ColumnNames = append(result.ColumnNames, currColName)
		result.ColumnValues = append(result.ColumnValues, val)
		result.OldKeys.KeyNames = append(result.OldKeys.KeyNames, currColName) // if not fill OldKeys - KeysChanged() will return 'true' when there are no pkey changing
		result.OldKeys.KeyTypes = append(result.OldKeys.KeyTypes, "stub")
		result.OldKeys.KeyValues = append(result.OldKeys.KeyValues, val)
		index++
	}
	eventUpdateKeys := maps.Keys(event.Update)
	sort.Strings(eventUpdateKeys)
	for _, currColName := range eventUpdateKeys {
		currColValue := event.Update[currColName]
		val, err := makeColNameColVal(schema.Columns(), colNameToIndex, currColName, currColValue)
		if err != nil {
			return nil, xerrors.Errorf("unable to make value from 'update', colName: %s, err: %w", currColName, err)
		}
		result.ColumnNames = append(result.ColumnNames, currColName)
		result.ColumnValues = append(result.ColumnValues, val)
		index++
	}
	eventNewImageKeys := maps.Keys(event.NewImage)
	sort.Strings(eventNewImageKeys)
	for _, currColName := range eventNewImageKeys {
		currColValue := event.NewImage[currColName]
		val, err := makeColNameColVal(schema.Columns(), colNameToIndex, currColName, currColValue)
		if err != nil {
			return nil, xerrors.Errorf("unable to make value from 'newImage', colName: %s, err: %w", currColName, err)
		}
		result.ColumnNames = append(result.ColumnNames, currColName)
		result.ColumnValues = append(result.ColumnValues, val)
		index++
	}
	if fillDefaults {
		resultNamesIds := result.ColumnNameIndices()
		eventSchemaKeys := schema.ColumnNames()
		for _, currColName := range eventSchemaKeys {
			if _, ok := resultNamesIds[currColName]; ok {
				continue
			}
			val, err := makeColNameColVal(schema.Columns(), colNameToIndex, currColName, nil)
			if err != nil {
				return nil, xerrors.Errorf("unable to make value from 'update', colName: %s, err: %w", currColName, err)
			}
			result.ColumnNames = append(result.ColumnNames, currColName)
			result.ColumnValues = append(result.ColumnValues, val)
			index++
		}
	}
	eventOldImage := maps.Keys(event.OldImage)
	sort.Strings(eventOldImage)
	for _, currColName := range eventOldImage {
		currColValue := event.OldImage[currColName]
		val, err := makeColNameColVal(schema.Columns(), colNameToIndex, currColName, currColValue)
		if err != nil {
			return nil, xerrors.Errorf("unable to make value from 'oldImage', colName: %s, err: %w", currColName, err)
		}
		result.OldKeys.KeyNames = append(result.OldKeys.KeyNames, currColName)
		result.OldKeys.KeyTypes = append(result.OldKeys.KeyTypes, "stub")
		result.OldKeys.KeyValues = append(result.OldKeys.KeyValues, val)
	}
	return result, nil
}

func makeDeleteChangeItem(
	tablePath string,
	schema *abstract.TableSchema,
	event *cdcEvent,
	writeTime time.Time,
	offset int64,
	partitionID int64,
	msgSize uint64,
) (*abstract.ChangeItem, error) {
	colNameToIndex := abstract.MakeMapColNameToIndex(schema.Columns())
	keyColumnNames := abstract.KeyNames(schema.Columns())
	result := &abstract.ChangeItem{
		ID:           0,
		LSN:          uint64(offset),
		CommitTime:   uint64(writeTime.UTC().UnixNano()),
		Counter:      0,
		Kind:         abstract.DeleteKind,
		Schema:       "",
		Table:        tablePath,
		PartID:       strconv.Itoa(int(partitionID)),
		ColumnNames:  nil,
		ColumnValues: nil,
		TableSchema:  schema,
		OldKeys: abstract.OldKeysType{
			KeyNames:  make([]string, 0, len(event.Key)),
			KeyTypes:  make([]string, 0, len(event.Key)),
			KeyValues: make([]interface{}, 0, len(event.Key)),
		},
		TxID:  "",
		Query: "",
		Size:  abstract.RawEventSize(msgSize),
	}
	index := 0
	for _, keyVal := range event.Key {
		if index >= len(keyColumnNames) {
			return nil, xerrors.Errorf("unable to handle changefeed event - wrong amount of pkey columns, index: %d, len(keyColumnNames): %d, keyColumnNames: %v", index, len(keyColumnNames), keyColumnNames)
		}
		currColName := keyColumnNames[index]
		val, err := makeColNameColVal(schema.Columns(), colNameToIndex, currColName, keyVal)
		if err != nil {
			return nil, xerrors.Errorf("unable to make value, keyName: %s, err: %w", currColName, err)
		}
		result.OldKeys.KeyNames = append(result.OldKeys.KeyNames, currColName)
		result.OldKeys.KeyTypes = append(result.OldKeys.KeyTypes, "stub")
		result.OldKeys.KeyValues = append(result.OldKeys.KeyValues, val)
		index++
	}
	eventOldImage := maps.Keys(event.OldImage)
	sort.Strings(eventOldImage)
	for _, currColName := range eventOldImage {
		currColValue := event.OldImage[currColName]
		val, err := makeColNameColVal(schema.Columns(), colNameToIndex, currColName, currColValue)
		if err != nil {
			return nil, xerrors.Errorf("unable to make value from 'oldImage', colName: %s, err: %w", currColName, err)
		}
		result.OldKeys.KeyNames = append(result.OldKeys.KeyNames, currColName)
		result.OldKeys.KeyTypes = append(result.OldKeys.KeyTypes, "stub")
		result.OldKeys.KeyValues = append(result.OldKeys.KeyValues, val)
	}
	return result, nil
}

func convertToChangeItem(
	tablePath string,
	schema *abstract.TableSchema,
	event *cdcEvent,
	writeTime time.Time,
	offset int64,
	partitionID int64,
	msgSize uint64,
	fillDefaults bool,
) (*abstract.ChangeItem, error) {
	if event.Update != nil {
		// insert/update
		result, err := makeUpdateChangeItem(tablePath, schema, event, writeTime, offset, partitionID, msgSize, fillDefaults)
		if err != nil {
			return nil, xerrors.Errorf("unable to make update changeItem, err: %w", err)
		}
		return result, nil
	} else if event.Erase != nil {
		// delete
		result, err := makeDeleteChangeItem(tablePath, schema, event, writeTime, offset, partitionID, msgSize)
		if err != nil {
			return nil, xerrors.Errorf("unable to make delete changeItem, err: %w", err)
		}
		return result, nil
	} else {
		return nil, xerrors.Errorf("unknown case: empty both: update & erase")
	}
}
