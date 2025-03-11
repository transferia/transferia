package snapshot

import (
	"database/sql"
	"strconv"

	"github.com/spf13/cast"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/providers/mysql/unmarshaller/types"
	"github.com/transferria/transferria/pkg/util/castx"
	"github.com/transferria/transferria/pkg/util/strict"
	"go.ytsaurus.tech/yt/go/schema"
	"golang.org/x/xerrors"
)

func unmarshalHetero(value interface{}, colSchema *abstract.ColSchema) (any, error) {
	if value == nil {
		return nil, nil
	}

	var result any
	var err error

	// in the switch below, the usage of `UnexpectedSQL` indicates an unexpected or even impossible situation.
	// However, in order for Data Transfer to remain resilient, "unexpected" casts must exist
	switch schema.Type(colSchema.DataType) {
	case schema.TypeInt64:
		result, err = strict.ExpectedSQL[*sql.NullInt64](value, cast.ToInt64E)
	case schema.TypeInt32:
		result, err = strict.ExpectedSQL[*sql.NullInt64](value, cast.ToInt32E)
	case schema.TypeInt16:
		result, err = strict.ExpectedSQL[*sql.NullInt64](value, cast.ToInt16E)
	case schema.TypeInt8:
		result, err = strict.ExpectedSQL[*sql.NullInt64](value, cast.ToInt8E)
	case schema.TypeUint64:
		result, err = strict.ExpectedSQL[*types.NullUint64](value, cast.ToUint64E)
	case schema.TypeUint32:
		result, err = strict.ExpectedSQL[*sql.NullInt64](value, cast.ToUint32E)
	case schema.TypeUint16:
		result, err = strict.ExpectedSQL[*sql.NullInt64](value, cast.ToUint16E)
	case schema.TypeUint8:
		result, err = strict.ExpectedSQL[*sql.NullInt64](value, cast.ToUint8E)
	case schema.TypeFloat32:
		result, err = strict.UnexpectedSQL(value, cast.ToFloat32E)
	case schema.TypeFloat64:
		switch v := value.(type) {
		case *sql.NullFloat64:
			result, err = strict.ExpectedSQL[*sql.NullFloat64](v, castx.ToJSONNumberE)
		case *sql.RawBytes:
			result, err = strict.Expected[[]byte](unmarshalRawBytesAsBytes(v), castx.ToJSONNumberE)
		default:
			result, err = strict.UnexpectedSQL(v, castx.ToJSONNumberE)
		}
	case schema.TypeBytes:
		switch v := value.(type) {
		case *sql.RawBytes:
			result, err = unmarshalRawBytesAsBytes(v), nil
		default:
			result, err = strict.UnexpectedSQL(v, castx.ToByteSliceE)
		}
	case schema.TypeBoolean:
		result, err = strict.UnexpectedSQL(value, cast.ToBoolE)
	case schema.TypeDate:
		result, err = strict.ExpectedSQL[*types.Temporal](value, cast.ToTimeE)
	case schema.TypeDatetime:
		result, err = strict.UnexpectedSQL(value, cast.ToTimeE)
	case schema.TypeTimestamp:
		result, err = strict.ExpectedSQL[*types.Temporal](value, cast.ToTimeE)
	case schema.TypeInterval:
		result, err = strict.UnexpectedSQL(value, cast.ToDurationE)
	case schema.TypeString:
		switch v := value.(type) {
		case *sql.RawBytes:
			result, err = strict.ExpectedSQL[[]byte](unmarshalRawBytesAsBytes(v), castx.ToStringE)
		case *sql.NullInt64:
			result, err = unmarshalInt64AsString(v)
		default:
			result, err = strict.UnexpectedSQL(v, castx.ToStringE)
		}
	case schema.TypeAny:
		result, err = strict.ExpectedSQL[*types.JSON](value, castx.ToJSONMarshallableE[any])
	default:
		return nil, abstract.NewFatalError(xerrors.Errorf("unexpected target type %s (original type %q, value of type %T), unmarshalling is not implemented", colSchema.DataType, colSchema.OriginalType, value))
	}
	logger.Log.Debugf("parsed %[1]v [%[1]T] into %[2]v [%[2]T]; error: %[3]v", value, result, err)

	if err != nil {
		return nil, abstract.NewStrictifyError(colSchema, schema.Type(colSchema.DataType), err)
	}
	return result, nil
}

func unmarshalRawBytesAsBytes(v *sql.RawBytes) any {
	if *v == nil {
		return nil
	}
	// https://st.yandex-team.ru/TM-6428 copying bytes here is REQUIRED
	result := make([]byte, len(*v))
	copy(result, *v)
	return result
}

func unmarshalInt64AsString(v *sql.NullInt64) (any, error) {
	extractionResult, err := strict.ExpectedSQL[*sql.NullInt64](v, cast.ToInt64E)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse int64 from NullInt64: %w", err)
	}
	if extractionResult == nil {
		return nil, nil
	}
	i64, ok := extractionResult.(int64)
	if !ok {
		return nil, xerrors.Errorf("ToInt64E returned a value of type %T", extractionResult)
	}
	return strconv.FormatInt(i64, 10), nil
}
