package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/postgres"
	yt_provider "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/migrate"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"golang.org/x/exp/constraints"
)

const (
	DummyIndexTable = "_dummy" // One day one guy made a huge mistake and now we have to live with it
	DummyMainTable  = "__dummy"
)

func MakeIndexTableName(originalName, idxCol string) string {
	return fmt.Sprintf("%s__idx_%s", originalName, idxCol)
}

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

func isSuperset(super, sub ytschema.Schema) bool {
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

func inferCommonPrimitiveType(lT, rT ytschema.Type) (ytschema.Type, error) {
	if lT == rT {
		return lT, nil
	}

	types := map[ytschema.Type]bool{lT: true, rT: true}

	switch {

	case types[ytschema.TypeInt64] && types[ytschema.TypeInt32]:
		return ytschema.TypeInt64, nil
	case types[ytschema.TypeInt64] && types[ytschema.TypeInt16]:
		return ytschema.TypeInt64, nil
	case types[ytschema.TypeInt64] && types[ytschema.TypeInt8]:
		return ytschema.TypeInt64, nil
	case types[ytschema.TypeInt32] && types[ytschema.TypeInt16]:
		return ytschema.TypeInt32, nil
	case types[ytschema.TypeInt32] && types[ytschema.TypeInt8]:
		return ytschema.TypeInt32, nil
	case types[ytschema.TypeInt16] && types[ytschema.TypeInt8]:
		return ytschema.TypeInt16, nil

	case types[ytschema.TypeUint64] && types[ytschema.TypeUint32]:
		return ytschema.TypeUint64, nil
	case types[ytschema.TypeUint64] && types[ytschema.TypeUint16]:
		return ytschema.TypeUint64, nil
	case types[ytschema.TypeUint64] && types[ytschema.TypeUint8]:
		return ytschema.TypeUint64, nil
	case types[ytschema.TypeUint32] && types[ytschema.TypeUint16]:
		return ytschema.TypeUint32, nil
	case types[ytschema.TypeUint32] && types[ytschema.TypeUint8]:
		return ytschema.TypeUint32, nil
	case types[ytschema.TypeUint16] && types[ytschema.TypeUint8]:
		return ytschema.TypeUint16, nil

	case types[ytschema.TypeBytes] && types[ytschema.TypeString]:
		return ytschema.TypeBytes, nil

	case types[ytschema.TypeAny]:
		return ytschema.TypeAny, nil

	default:
		return lT, xerrors.Errorf("cannot infer common type for: %v and %v", lT.String(), rT.String())
	}
}

func inferCommonComplexType(lT, rT ytschema.ComplexType) (ytschema.ComplexType, error) {
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
		return ytschema.Optional{Item: commonPrimitive}, nil
	}
	return commonPrimitive, nil
}

func extractType(ct ytschema.ComplexType) (ytschema.Type, error) {
	switch t := ct.(type) {
	case ytschema.Optional:
		return t.Item.(ytschema.Type), nil
	case ytschema.Type:
		return t, nil
	default:
		return "", xerrors.Errorf("got unsupported type_v3 complex type: %T", t)
	}
}

func isOptional(ct ytschema.ComplexType) bool {
	_, ok := ct.(ytschema.Optional)
	return ok
}

func inferCommonRequireness(lR, rR bool) bool {
	return lR && rR
}

func compatiblePKey(current, expected ytschema.Schema) bool {
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

func mergeColumns(lC, rC ytschema.Column) (ytschema.Column, error) {
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

func inferCommonType(lC, rC ytschema.Column) (ytschema.ComplexType, error) {
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
		return ytschema.Optional{Item: commonType}, nil
	}

	return nil, xerrors.New("columns have uncompatible typing: both must have ComplexType or old Type")
}

func unionSchemas(current, expected ytschema.Schema) (ytschema.Schema, error) {
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

	keyColumns := make([]ytschema.Column, 0)
	notRequiredColumns := make([]ytschema.Column, 0)

	currentColumns := map[string]ytschema.Column{}
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

			if mergedCol.SortOrder != ytschema.SortNone {
				keyColumns = append(keyColumns, mergedCol)
			} else {
				notRequiredColumns = append(notRequiredColumns, mergedCol)
			}
		} else {
			col.Required = false
			_ = col.NormalizeType()
			if !isOptional(col.ComplexType) {
				col.ComplexType = ytschema.Optional{Item: col.ComplexType}
			}

			notRequiredColumns = append(notRequiredColumns, col)
		}
	}

	// preserve order of deleted non key columns to avoid unnecessary alters if old rows would be inserted
	for _, col := range current.Columns {
		_, notAdded := currentColumns[col.Name]
		if notAdded {
			col.Required = false
			_ = col.NormalizeType()
			if !isOptional(col.ComplexType) {
				col.ComplexType = ytschema.Optional{Item: col.ComplexType}
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
	return func(path ypath.Path, actual, expected ytschema.Schema) error {
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

		if err := yt_provider.MountUnmountWrapper(ctx, ytClient, path, migrate.UnmountAndWait); err != nil {
			return xerrors.Errorf("unmount error: %w", err)
		}
		if err := ytClient.AlterTable(ctx, path, &yt.AlterTableOptions{Schema: &unitedSchema}); err != nil {
			return xerrors.Errorf("alter error: %w", err)
		}
		if err := yt_provider.MountUnmountWrapper(ctx, ytClient, path, migrate.MountAndWait); err != nil {
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

func RestoreWithLengthLimitCheck(colSchema abstract.ColSchema, val any, ignoreBigVals bool, lengthLimit int) (any, error) {
	res, err := restore(colSchema, val, lengthLimit == YtStatMaxStringLength)
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
	default:
	}
	return res, nil
}

func restore(colSchema abstract.ColSchema, val any, isStatic bool) (any, error) {
	if val == nil {
		return val, nil
	}
	if reflect.ValueOf(val).Kind() == reflect.Pointer {
		restored, err := restore(colSchema, reflect.ValueOf(val).Elem().Interface(), isStatic)
		if err != nil {
			return nil, xerrors.Errorf("unable to restore from ptr: %w", err)
		}
		return restored, nil
	}

	// hacks for not-strictified sources ('pg:timestamp with time zone', 'ch:Float32') AND for our ugly solutions (enums in pg)
	if colSchema.OriginalType == "ch:Float32" || colSchema.OriginalType == "ch:Nullable(Float32)" {
		switch v := val.(type) {
		case float32:
			return v, nil
		case json.Number:
			res, err := v.Float64()
			if err != nil {
				return nil, xerrors.Errorf("unable to restore from ch:Float32, err: %w", err)
			}
			return float32(res), nil
		default:
			return nil, xerrors.Errorf("unable type for ch:Float32, type=%T", val)
		}
	}
	if postgres.IsPgEnum(colSchema) {
		switch v := val.(type) {
		case string:
			return v, nil
		case []interface{}:
			//nolint:descriptiveerrors
			return newAnyWrapper(v)
		default:
			return nil, xerrors.Errorf("unknown pg:enum type: %T", val)
		}
	}

	// it's for TM-4877
	if (colSchema.OriginalType == "pg:timestamp with time zone" || colSchema.OriginalType == "pg:timestamp without time zone") && colSchema.DataType == ytschema.TypeString.String() {
		v, ok := val.(time.Time)
		if !ok {
			return nil, xerrors.Errorf("unable type for pg:timestamp with time zone, type=%T", val)
		}
		if isStatic {
			return v.Format(time.RFC3339Nano), nil
		} else {
			//nolint:descriptiveerrors
			return timeCaster(v, colSchema.DataType, ytschema.NewTimestamp)
		}
	}

	// some old ugly hack
	if colSchema.DataType == string(ytschema.TypeInt64) {
		if v, ok := val.(time.Time); ok {
			return -v.UnixNano(), nil
		}
	}

	switch colSchema.DataType {
	case ytschema.TypeInt8.String(), ytschema.TypeInt16.String(), ytschema.TypeInt32.String(), ytschema.TypeInt64.String():
		//nolint:descriptiveerrors
		return doNumberConversion[int64](val, colSchema.DataType)
	case ytschema.TypeUint8.String(), ytschema.TypeUint16.String(), ytschema.TypeUint32.String(), ytschema.TypeUint64.String():
		//nolint:descriptiveerrors
		return doNumberConversion[uint64](val, colSchema.DataType)
	case ytschema.TypeFloat32.String(), ytschema.TypeFloat64.String():
		switch v := val.(type) {
		case json.Number:
			res, err := v.Float64()
			if err != nil {
				return nil, xerrors.Errorf("unable to parse float64 from json number: %w", err)
			}
			return res, nil
		default:
			//nolint:descriptiveerrors
			return doNumberConversion[float64](v, colSchema.DataType)
		}
	case ytschema.TypeBytes.String(), ytschema.TypeString.String():
		//nolint:descriptiveerrors
		return doTextConversion(val, colSchema.DataType)
	case ytschema.TypeBoolean.String():
		switch v := val.(type) {
		case bool:
			return v, nil
		default:
			return nil, xerrors.Errorf("unknown data type for TypeBoolean: %T", val)
		}
	case ytschema.TypeAny.String():
		if isStatic {
			return val, nil
		}
		if colSchema.PrimaryKey {
			// TM-2118 TM-1893 DTSUPPORT-594 if primary key, should be marshalled independently to prevent "122" == "\"122\""
			stringifiedJSON, err := json.Marshal(val)
			if err != nil {
				return nil, xerrors.Errorf("unable to marshal pkey json: %w", err)
			}
			return stringifiedJSON, nil
		}
		//nolint:descriptiveerrors
		return newAnyWrapper(val)
	case ytschema.TypeDate.String():
		//nolint:descriptiveerrors
		return timeCaster(val, colSchema.DataType, ytschema.NewDate)
	case ytschema.TypeDatetime.String():
		//nolint:descriptiveerrors
		return timeCaster(val, colSchema.DataType, ytschema.NewDatetime)
	case ytschema.TypeTimestamp.String():
		//nolint:descriptiveerrors
		return timeCaster(val, colSchema.DataType, ytschema.NewTimestamp)
	case ytschema.TypeInterval.String():
		switch v := val.(type) {
		case int64:
			return v, nil
		case time.Duration:
			asInterval, err := ytschema.NewInterval(v)
			if err != nil {
				return nil, xerrors.Errorf("unable to create interval: %w", err)
			}
			return asInterval, nil
		default:
			return nil, xerrors.Errorf("unknown data type for TypeInterval: %T", val)
		}
	default:
		return nil, xerrors.Errorf("unknown colSchema.DataType: %s", colSchema.DataType)
	}
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

func timeCaster[T any](val any, dateType string, caster func(time.Time) (T, error)) (any, error) {
	switch v := val.(type) {
	case time.Time:
		casted, err := castTimeWithDataLoss[T](v, caster)
		if err != nil {
			return nil, xerrors.Errorf("unable to create %s: %w", dateType, err)
		}
		return casted, nil
	case uint64:
		return v, nil
	default:
		return nil, xerrors.Errorf("unknown data type for %s: %T", dateType, val)
	}
}

// castTimeWithDataLoss tries to cast value and trims time if it not fits into YT's range. TODO: Remove in TM-7874.
func castTimeWithDataLoss[T any](value time.Time, caster func(time.Time) (T, error)) (T, error) {
	var rangeErr *ytschema.RangeError
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
