package sink

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	yt2 "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"golang.org/x/exp/constraints"
)

type IncompatibleSchemaErr struct{ error }

func (u IncompatibleSchemaErr) Unwrap() error {
	return u.error
}

func (u IncompatibleSchemaErr) Is(err error) bool {
	_, ok := err.(IncompatibleSchemaErr)
	return ok
}

func IsIncompatibleSchemaErr(err error) bool {
	return xerrors.Is(err, IncompatibleSchemaErr{error: err})
}

func NewIncompatibleSchemaErr(err error) *IncompatibleSchemaErr {
	return &IncompatibleSchemaErr{error: err}
}

var NoKeyColumnsFound = xerrors.New("No key columns found")

func isSuperset(super, sub schema.Schema) bool {
	if len(super.Columns) < len(sub.Columns) {
		return false
	}

	i, j := 0, 0
	intersection := super
	intersection.Columns = nil
	for i < len(super.Columns) && j < len(sub.Columns) {
		if super.Columns[i].Name == sub.Columns[j].Name {
			intersection = intersection.Append(super.Columns[i])
			i++
			j++
		} else {
			i++
		}
	}
	return intersection.Equal(sub)
}

func inferCommonPrimitiveType(lT, rT schema.Type) (schema.Type, error) {
	if lT == rT {
		return lT, nil
	}

	types := map[schema.Type]bool{lT: true, rT: true}

	switch {

	case types[schema.TypeInt64] && types[schema.TypeInt32]:
		return schema.TypeInt64, nil
	case types[schema.TypeInt64] && types[schema.TypeInt16]:
		return schema.TypeInt64, nil
	case types[schema.TypeInt64] && types[schema.TypeInt8]:
		return schema.TypeInt64, nil
	case types[schema.TypeInt32] && types[schema.TypeInt16]:
		return schema.TypeInt32, nil
	case types[schema.TypeInt32] && types[schema.TypeInt8]:
		return schema.TypeInt32, nil
	case types[schema.TypeInt16] && types[schema.TypeInt8]:
		return schema.TypeInt16, nil

	case types[schema.TypeUint64] && types[schema.TypeUint32]:
		return schema.TypeUint64, nil
	case types[schema.TypeUint64] && types[schema.TypeUint16]:
		return schema.TypeUint64, nil
	case types[schema.TypeUint64] && types[schema.TypeUint8]:
		return schema.TypeUint64, nil
	case types[schema.TypeUint32] && types[schema.TypeUint16]:
		return schema.TypeUint32, nil
	case types[schema.TypeUint32] && types[schema.TypeUint8]:
		return schema.TypeUint32, nil
	case types[schema.TypeUint16] && types[schema.TypeUint8]:
		return schema.TypeUint16, nil

	case types[schema.TypeBytes] && types[schema.TypeString]:
		return schema.TypeBytes, nil

	case types[schema.TypeAny]:
		return schema.TypeAny, nil

	default:
		return lT, xerrors.Errorf("cannot infer common type for: %v and %v", lT.String(), rT.String())
	}
}

func inferCommonComplexType(lT, rT schema.ComplexType) (schema.ComplexType, error) {
	lPrimitive, err := extractType(lT)
	if err != nil {
		//nolint:descriptiveerrors
		return nil, err
	}

	rPrimitive, err := extractType(rT)
	if err != nil {
		//nolint:descriptiveerrors
		return nil, err
	}

	commonPrimitive, err := inferCommonPrimitiveType(lPrimitive, rPrimitive)
	if err != nil {
		return nil, xerrors.Errorf("uncompatible underlaying types: %w", err)
	}

	if isOptional(lT) || isOptional(rT) {
		return schema.Optional{Item: commonPrimitive}, nil
	}
	return commonPrimitive, nil
}

func extractType(ct schema.ComplexType) (schema.Type, error) {
	switch t := ct.(type) {
	case schema.Optional:
		return t.Item.(schema.Type), nil
	case schema.Type:
		return t, nil
	default:
		return "", xerrors.Errorf("got unsupported type_v3 complex type: %T", t)
	}
}

func isOptional(ct schema.ComplexType) bool {
	_, ok := ct.(schema.Optional)
	return ok
}

func inferCommonRequireness(lR, rR bool) bool {
	return lR && rR
}

func compatiblePKey(current, expected schema.Schema) bool {
	currentKey := current.KeyColumns()
	expectedKey := expected.KeyColumns()

	if len(expectedKey) < len(currentKey) {
		return false
	}

	for i := range currentKey {
		if currentKey[i] != expectedKey[i] {
			return false
		}
	}
	return true
}

func mergeColumns(lC, rC schema.Column) (schema.Column, error) {
	commonType, err := inferCommonType(lC, rC)
	if err != nil {
		return lC, xerrors.Errorf("cannot infer common type for column %v: %w", lC.Name, err)
	}
	lC.ComplexType = commonType
	_ = lC.NormalizeType()
	if lC.SortOrder != rC.SortOrder {
		return lC, xerrors.Errorf("cannot add existed column to key: %v", lC.Name)
	}
	return lC, nil
}

func inferCommonType(lC, rC schema.Column) (schema.ComplexType, error) {
	if lC.ComplexType != nil && rC.ComplexType != nil {
		//nolint:descriptiveerrors
		return inferCommonComplexType(lC.ComplexType, rC.ComplexType)
	}

	if lC.Type != "" && rC.Type != "" {
		commonType, err := inferCommonPrimitiveType(lC.Type, rC.Type)
		if err != nil {
			//nolint:descriptiveerrors
			return nil, err
		}
		bothRequired := inferCommonRequireness(lC.Required, rC.Required)
		if bothRequired {
			return commonType, nil
		}
		return schema.Optional{Item: commonType}, nil
	}

	return nil, xerrors.New("columns have uncompatible typing: both must have ComplexType or old Type")
}

func unionSchemas(current, expected schema.Schema) (schema.Schema, error) {
	if !compatiblePKey(current, expected) {
		return current, xerrors.Errorf("incompatible key change: %w", NewIncompatibleSchemaErr(
			xerrors.Errorf("changed order or some columns were deleted from key: current key: %v, expected key: %v",
				current.KeyColumns(),
				expected.KeyColumns(),
			),
		),
		)
	}

	union := current
	union.Columns = nil

	keyColumns := make([]schema.Column, 0)
	notRequiredColumns := make([]schema.Column, 0)

	currentColumns := map[string]schema.Column{}
	for _, col := range current.Columns {
		currentColumns[col.Name] = col
	}

	for _, col := range expected.Columns {
		curCol, curOk := currentColumns[col.Name]
		if curOk {
			delete(currentColumns, col.Name)
			mergedCol, err := mergeColumns(col, curCol)
			if err != nil {
				return expected, err
			}

			if mergedCol.SortOrder != schema.SortNone {
				keyColumns = append(keyColumns, mergedCol)
			} else {
				notRequiredColumns = append(notRequiredColumns, mergedCol)
			}
		} else {
			col.Required = false
			_ = col.NormalizeType()
			if !isOptional(col.ComplexType) {
				col.ComplexType = schema.Optional{Item: col.ComplexType}
			}

			notRequiredColumns = append(notRequiredColumns, col)
		}
	}

	//preserve order of deleted non key columns to avoid unnecessary alters if old rows would be inserted
	for _, col := range current.Columns {
		_, notAdded := currentColumns[col.Name]
		if notAdded {
			col.Required = false
			_ = col.NormalizeType()
			if !isOptional(col.ComplexType) {
				col.ComplexType = schema.Optional{Item: col.ComplexType}
			}
			notRequiredColumns = append(notRequiredColumns, col)
		}
	}

	for _, col := range keyColumns {
		union = union.Append(col)
	}
	for _, col := range notRequiredColumns {
		union = union.Append(col)
	}

	return union, nil
}

func onConflictTryAlterWithoutNarrowing(ctx context.Context, ytClient yt.Client) migrate.ConflictFn {
	return func(path ypath.Path, actual, expected schema.Schema) error {
		logger.Log.Info("table schema conflict detected", log.String("path", path.String()), log.Reflect("expected", expected), log.Reflect("actual", actual))
		if isSuperset(actual, expected) {
			// No error, do not retry schema comparison
			logger.Log.Info("actual schema is superset of the expected; proceeding without alter", log.String("path", path.String()))
			return nil
		}

		unitedSchema, err := unionSchemas(actual, expected)
		if err != nil {
			return xerrors.Errorf("got incompatible schema changes in '%s': %w", path.String(), err)
		}
		logger.Log.Info("united schema computed", log.String("path", path.String()), log.Reflect("united_schema", unitedSchema))

		if err := yt2.MountUnmountWrapper(ctx, ytClient, path, migrate.UnmountAndWait); err != nil {
			return xerrors.Errorf("unmount error: %w", err)
		}
		if err := ytClient.AlterTable(ctx, path, &yt.AlterTableOptions{Schema: &unitedSchema}); err != nil {
			return xerrors.Errorf("alter error: %w", err)
		}
		if err := yt2.MountUnmountWrapper(ctx, ytClient, path, migrate.MountAndWait); err != nil {
			return xerrors.Errorf("mount error: %w", err)
		}
		// Schema has been altered, no need to retry schema comparison
		logger.Log.Info("schema altered", log.String("path", path.String()))
		return nil
	}
}

func beginTabletTransaction(ctx context.Context, ytClient yt.Client, fullAtomicity bool, logger log.Logger) (yt.TabletTx, util.Rollbacks, error) {
	txOpts := &yt.StartTabletTxOptions{Atomicity: &yt.AtomicityFull}
	if !fullAtomicity {
		txOpts.Atomicity = &yt.AtomicityNone
	}
	var rollbacks util.Rollbacks
	tx, err := ytClient.BeginTabletTx(ctx, txOpts)
	if err != nil {
		return nil, rollbacks, err
	}
	rollbacks.Add(func() {
		if err := tx.Abort(); err != nil {
			logger.Warn("Unable to abort transaction", log.Error(err))
		}
	})
	return tx, rollbacks, nil
}

const (
	YtDynMaxStringLength  = 16 * 1024 * 1024  // https://yt.yandex-team.ru/docs/description/dynamic_tables/dynamic_tables_overview#limitations
	YtStatMaxStringLength = 128 * 1024 * 1024 // https://yt.yandex-team.ru/docs/user-guide/storage/static-tables#limitations
	MagicString           = "BigStringValueStub"
)

type rpcAnyWrapper struct {
	ysonVal []byte
}

func (w rpcAnyWrapper) MarshalYSON() ([]byte, error) {
	return w.ysonVal, nil
}

func newAnyWrapper(val any) (*rpcAnyWrapper, error) {
	res, err := yson.Marshal(val)
	if err != nil {
		return nil, err
	}
	return &rpcAnyWrapper{ysonVal: res}, nil
}

func RestoreWithLengthLimitCheck(colSchema abstract.ColSchema, val interface{}, ignoreBigVals bool, lengthLimit int) (interface{}, error) {
	res, err := restore(colSchema, val)
	if err != nil {
		//nolint:descriptiveerrors
		return res, err
	}
	switch v := res.(type) {
	case *rpcAnyWrapper:
		if len(v.ysonVal) > lengthLimit {
			if ignoreBigVals {
				//nolint:descriptiveerrors
				return newAnyWrapper(MagicString)
			}
			return res, xerrors.Errorf("string of type %v is larger than allowed for dynamic table size", colSchema.DataType)
		}
	case []byte:
		if len(v) > lengthLimit {
			if ignoreBigVals {
				return []byte(MagicString), nil
			}
			return res, xerrors.Errorf("string of type %v is larger than allowed for dynamic table size", colSchema.DataType)
		}
	case string:
		if len(v) > lengthLimit {
			if ignoreBigVals {
				return MagicString, nil
			}
			return res, xerrors.Errorf("string of type %v is larger than allowed for dynamic table size", colSchema.DataType)
		}
	}
	return res, nil
}

func restore(colSchema abstract.ColSchema, val interface{}) (interface{}, error) {
	if val == nil {
		return val, nil
	}
	if reflect.ValueOf(val).Kind() == reflect.Pointer {
		restored, err := restore(colSchema, reflect.ValueOf(val).Elem().Interface())
		if err != nil {
			return nil, xerrors.Errorf("unable to restore from ptr: %w", err)
		}
		return restored, nil
	}

	if colSchema.PrimaryKey && strings.Contains(colSchema.OriginalType, "json") {
		// TM-2118 TM-1893 DTSUPPORT-594 if primary key, should be marshalled independently to prevent "122" == "\"122\""
		stringifiedJSON, err := json.Marshal(val)
		if err != nil {
			return nil, xerrors.Errorf("unable to marshal pkey json: %w", err)
		}
		return stringifiedJSON, nil
	}

	switch v := val.(type) {
	case time.Time:
		switch strings.ToLower(colSchema.DataType) {
		case string(schema.TypeTimestamp):
			casted, err := castTimeWithDataLoss(v, schema.NewTimestamp)
			if err != nil {
				return nil, xerrors.Errorf("unable to create Timestamp: %w", err)
			}
			return casted, nil

		case string(schema.TypeDate):
			casted, err := castTimeWithDataLoss(v, schema.NewDate)
			if err != nil {
				return nil, xerrors.Errorf("unable to create Date: %w", err)
			}
			return casted, nil

		case string(schema.TypeDatetime):
			casted, err := castTimeWithDataLoss(v, schema.NewDatetime)
			if err != nil {
				return nil, xerrors.Errorf("unable to create Datetime: %w", err)
			}
			return casted, nil

		case string(schema.TypeInt64):
			return -v.UnixNano(), nil
		}

	case json.Number:
		var res any
		var err error
		if colSchema.OriginalType == "mysql:json" {
			res = v
		} else {
			res, err = v.Float64()
			if err != nil {
				return nil, xerrors.Errorf("unable to parse float64 from json number: %w", err)
			}
		}
		if colSchema.DataType == schema.TypeAny.String() {
			//nolint:descriptiveerrors
			return newAnyWrapper(res)
		}
		return res, nil

	case time.Duration:
		asInterval, err := schema.NewInterval(v)
		if err != nil {
			return nil, xerrors.Errorf("unable to create interval: %w", err)
		}
		return asInterval, nil

	default:
		ytType := strings.ToLower(colSchema.DataType)
		switch ytType {
		case string(schema.TypeInt64), string(schema.TypeInt32), string(schema.TypeInt16), string(schema.TypeInt8):
			//nolint:descriptiveerrors
			return doNumberConversion[int64](val, ytType)
		case string(schema.TypeUint64), string(schema.TypeUint32), string(schema.TypeUint16), string(schema.TypeUint8):
			//nolint:descriptiveerrors
			return doNumberConversion[uint64](val, ytType)
		case string(schema.TypeFloat32), string(schema.TypeFloat64):
			//nolint:descriptiveerrors
			return doNumberConversion[float64](val, ytType)
		case string(schema.TypeBytes), string(schema.TypeString):
			//nolint:descriptiveerrors
			return doTextConversion(val, ytType)
		case string(schema.TypeBoolean):
			converted, ok := val.(bool)
			if !ok {
				return nil, xerrors.Errorf("unaccepted value %v for yt type %s", val, ytType)
			}
			return converted, nil
		case string(schema.TypeDate), string(schema.TypeDatetime), string(schema.TypeTimestamp):
			converted, ok := val.(uint64)
			if !ok {
				return nil, xerrors.Errorf("unaccepted value %v for yt type %s", val, ytType)
			}
			return converted, nil
		case string(schema.TypeInterval):
			converted, ok := val.(int64)
			if !ok {
				return nil, xerrors.Errorf("unaccepted value %v for yt type %s", val, ytType)
			}
			return converted, nil
		}
	}

	if colSchema.PrimaryKey && colSchema.DataType == schema.TypeAny.String() { // YT not support yson as primary key
		switch v := val.(type) {
		case string:
			return v, nil
		default:
			bytes, err := yson.Marshal(val)
			if err != nil {
				return nil, xerrors.Errorf("unable to marshal item's value of type '%T': %w", val, err)
			}
			return string(bytes), nil
		}
	}

	res := abstract.Restore(colSchema, val)
	if colSchema.DataType == schema.TypeAny.String() {
		//nolint:descriptiveerrors
		return newAnyWrapper(res)
	}
	return res, nil
}

type Number interface {
	constraints.Integer | constraints.Float
}

func doNumberConversion[T Number](val interface{}, ytType string) (T, error) {
	switch v := val.(type) {
	case int:
		return T(v), nil
	case int8:
		return T(v), nil
	case int16:
		return T(v), nil
	case int32:
		return T(v), nil
	case int64:
		return T(v), nil
	case uint:
		return T(v), nil
	case uint8:
		return T(v), nil
	case uint16:
		return T(v), nil
	case uint32:
		return T(v), nil
	case uint64:
		return T(v), nil
	case float32:
		return T(v), nil
	case float64:
		return T(v), nil
	}
	return *new(T), xerrors.Errorf("unaccepted value %v for yt type %v", val, ytType)
}

func doTextConversion(val interface{}, ytType string) (string, error) {
	switch v := val.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case byte:
		return string(v), nil
	}
	return "", xerrors.Errorf("unaccepted value %v for yt type %v", val, ytType)
}

// TODO: Completely remove this legacy hack
func fixDatetime(c *abstract.ColSchema) schema.Type {
	return schema.Type(strings.ToLower(c.DataType))
}

func schemasAreEqual(current, received []abstract.ColSchema) bool {
	if len(current) != len(received) {
		return false
	}

	currentSchema := make(map[string]abstract.ColSchema)
	for _, col := range current {
		currentSchema[col.ColumnName] = col
	}

	for _, col := range received {
		tCol, ok := currentSchema[col.ColumnName]
		if !ok || tCol.PrimaryKey != col.PrimaryKey || tCol.DataType != col.DataType {
			return false
		}
		delete(currentSchema, col.ColumnName)
	}

	return true
}

// castTimeWithDataLoss tries to cast value and trims time if it not fits into YT's range. TODO: Remove in TM-7874.
func castTimeWithDataLoss[T any](value time.Time, caster func(time.Time) (T, error)) (T, error) {
	var rangeErr *schema.RangeError
	var nilT T // Used as return value if unexpected error occures.

	casted, err := caster(value)
	if err == nil || !xerrors.As(err, &rangeErr) {
		// If error is nil, or it is not RangeError – castTimeWithDataLoss behaves just like caster.
		return casted, err
	}

	// Unsuccessful cast because of RangeError, extract available range from error and trim value.
	minTime, minOk := rangeErr.MinValue.(time.Time)
	maxTime, maxOk := rangeErr.MaxValue.(time.Time)
	if !minOk || !maxOk {
		msg := "unable to extract range bounds, got (%T, %T) instead of (time.Time, time.Time) from RangeError = '%w'"
		return nilT, xerrors.Errorf(msg, value, minTime, maxTime, err)
	}

	if value.Before(minTime) {
		value = minTime
	} else if value.After(maxTime) {
		value = maxTime
	}

	casted, err = caster(value)
	if err != nil {
		return nilT, xerrors.Errorf("unable to cast time '%v': %w", value, err)
	}
	return casted, nil
}
