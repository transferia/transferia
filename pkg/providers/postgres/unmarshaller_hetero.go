//go:build !disable_postgres_provider

package postgres

import (
	"bytes"
	"database/sql/driver"
	"net"
	"time"

	"github.com/gofrs/uuid"
	"github.com/jackc/pgtype"
	"github.com/spf13/cast"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util/castx"
	"github.com/transferia/transferia/pkg/util/jsonx"
	"github.com/transferia/transferia/pkg/util/strict"
	"go.ytsaurus.tech/yt/go/schema"
)

func unmarshalFieldHetero(value any, colSchema *abstract.ColSchema, connInfo *pgtype.ConnInfo) (any, error) {
	var result any
	var err error

	// in the switch below, the usage of `strict.UnexpectedSQL` indicates an unexpected or even impossible situation.
	// However, in order for Data Transfer to remain resilient, "unexpected" casts must exist
	switch schema.Type(colSchema.DataType) {
	case schema.TypeInt64:
		result, err = strict.ExpectedSQL[*pgtype.Int8](value, cast.ToInt64E)
	case schema.TypeInt32:
		result, err = strict.ExpectedSQL[*pgtype.Int4](value, cast.ToInt32E)
	case schema.TypeInt16:
		result, err = strict.ExpectedSQL[*pgtype.Int2](value, cast.ToInt16E)
	case schema.TypeInt8:
		result, err = strict.UnexpectedSQL(value, cast.ToInt8E)
	case schema.TypeUint64:
		result, err = strict.UnexpectedSQL(value, cast.ToUint64E)
	case schema.TypeUint32:
		result, err = strict.UnexpectedSQL(value, cast.ToUint32E)
	case schema.TypeUint16:
		result, err = strict.UnexpectedSQL(value, cast.ToUint16E)
	case schema.TypeUint8:
		result, err = strict.UnexpectedSQL(value, cast.ToUint8E)
	case schema.TypeFloat32:
		result, err = strict.UnexpectedSQL(value, cast.ToFloat32E)
	case schema.TypeFloat64:
		switch v := value.(type) {
		case *pgtype.Float4:
			result, err = strict.ExpectedSQL[*pgtype.Float4](v, castx.ToJSONNumberE)
		case *pgtype.Float8:
			result, err = strict.ExpectedSQL[*pgtype.Float8](v, castx.ToJSONNumberE)
		case *pgtype.Numeric:
			result, err = strict.ExpectedSQL[*pgtype.Numeric](v, castx.ToJSONNumberE)
		default:
			result, err = strict.UnexpectedSQL(v, castx.ToJSONNumberE)
		}
	case schema.TypeBytes:
		result, err = strict.ExpectedSQL[*pgtype.Bytea](value, castx.ToByteSliceE)
	case schema.TypeBoolean:
		result, err = strict.ExpectedSQL[*pgtype.Bool](value, cast.ToBoolE)
	case schema.TypeDate:
		result, err = strict.ExpectedSQL[*Date](value, cast.ToTimeE)
	case schema.TypeDatetime:
		result, err = strict.UnexpectedSQL(value, cast.ToTimeE)
	case schema.TypeTimestamp:
		switch v := value.(type) {
		case *Timestamp:
			result, err = strict.ExpectedSQL[*Timestamp](v, cast.ToTimeE)
		case *Timestamptz:
			result, err = strict.ExpectedSQL[*Timestamptz](v, cast.ToTimeE)
		default:
			result, err = strict.UnexpectedSQL(value, cast.ToTimeE)
		}
	case schema.TypeInterval:
		result, err = strict.UnexpectedSQL(value, cast.ToDurationE)
	case schema.TypeString:
		result, err = expectedStringCast(value)
	case schema.TypeAny:
		result, err = expectedAnyCast(value, colSchema, connInfo)
	default:
		return nil, abstract.NewFatalError(xerrors.Errorf("unexpected target type %s (original type %q, value of type %T), unmarshalling is not implemented", colSchema.DataType, colSchema.OriginalType, value))
	}

	if err != nil {
		return nil, abstract.NewStrictifyError(colSchema, schema.Type(colSchema.DataType), err)
	}
	return result, nil
}

func expectedAnyCast(value any, colSchema *abstract.ColSchema, connInfo *pgtype.ConnInfo) (any, error) {
	var result any
	var err error

	// JSON marshallability for all types will be additionally ensured later
	switch v := value.(type) {
	case *pgtype.CompositeType:
		result, err = unmarshalComposite(v, connInfo)
	case *GenericArray:
		result, err = unmarshalGenericArray(v, colSchema, connInfo)
	case *pgtype.GenericText:
		result, err = strict.ExpectedSQL[*pgtype.GenericText](v, castx.ToStringE)
	case *pgtype.Varbit:
		result, err = strict.ExpectedSQL[*pgtype.Varbit](v, castx.ToStringE)
	case *pgtype.Bit:
		result, err = strict.ExpectedSQL[*pgtype.Varbit]((*pgtype.Varbit)(v), castx.ToStringE)
	case *pgtype.Record:
		result, err = unmarshalRecord(v)
	case *pgtype.JSON:
		result, err = unmarshalJSON(v)
	case *pgtype.JSONB:
		result, err = unmarshalJSON((*pgtype.JSON)(v))
	case *pgtype.CIDR:
		result, err = unmarshalCIDR(v)
	case *pgtype.Inet:
		result, err = strict.ExpectedSQL[*pgtype.Inet](v, castx.ToStringE)
	case *pgtype.Tstzrange:
		result, err = strict.ExpectedSQL[*pgtype.Tstzrange](v, castx.ToStringE)
	case *pgtype.Daterange:
		result, err = unmarshalDaterange(v)
	case driver.Valuer:
		// all possible pgtype types (which can appear in future PostgreSQL versions and those which Transfer just does not support) should fall into this case
		result, err = v.Value()
	case [16]byte:
		// this should never appear, but has been present since https://github.com/transferia/transferia/review/757334/files/2#file-/trunk/arcadia/transfer_manager/go/pkg/storage/pg/scanner.go:R21
		result, err = uuid.FromBytesOrNil(v[:16]).String(), nil
	case *net.IPNet:
		// this should never appear, but has been present since https://github.com/transferia/transferia/review/1204645/files/2#file-transfer_manager/go/pkg/storage/pg/scanner.go:R98
		result, err = unmarshalIPNet(v)
	case net.IPNet:
		// this should never appear, but has been present since https://github.com/transferia/transferia/review/1204645/files/2#file-transfer_manager/go/pkg/storage/pg/scanner.go:R98
		result, err = unmarshalIPNet(&v)
	case byte:
		// unclear what this is for, but has been present since https://github.com/transferia/transferia/review/757334/files/2#file-/trunk/arcadia/transfer_manager/go/pkg/storage/pg/scanner.go:R19
		result, err = int(v), nil
	default:
		// ARRAY elements should fall into this case, they are normally basic Go types
		result, err = v, nil
	}

	if err != nil {
		return nil, xerrors.Errorf("failed to cast %T to any: %w", value, err)
	}
	resultJS, err := ensureJSONMarshallable(result)
	if err != nil {
		return nil, xerrors.Errorf("successfully casted %T to any (%T), but the result is not JSON-serializable: %w", value, resultJS, err)
	}
	return resultJS, nil
}

func unmarshalComposite(v *pgtype.CompositeType, connInfo *pgtype.ConnInfo) (any, error) {
	buf, err := v.EncodeText(connInfo, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to encode composite type as text: %w", err)
	}
	if buf == nil {
		return nil, nil
	}
	return string(buf), nil
}

func unmarshalGenericArray(v *GenericArray, colSchema *abstract.ColSchema, connInfo *pgtype.ConnInfo) (any, error) {
	maybeSlice, err := v.ExtractValue(connInfo)
	if err != nil {
		return nil, xerrors.Errorf("failed to extract a value from a generic array: %w", err)
	}
	if maybeSlice == nil {
		return nil, nil
	}
	switch extracted := maybeSlice.(type) {
	case string:
		return extracted, nil
	case []any:
		for i, el := range extracted {
			v, err := unmarshalFieldHetero(el, colSchema, connInfo)
			if err != nil {
				return nil, xerrors.Errorf("failed to unmarshal a value in an array at position [%d]: %w", i, err)
			}
			extracted[i] = v
		}
		return extracted, nil
	default:
		return nil, xerrors.Errorf("extracted value is of unexpected type %T", extracted)
	}
}

func unmarshalRecord(v *pgtype.Record) (any, error) {
	switch v.Status {
	case pgtype.Null:
		return nil, nil
	case pgtype.Undefined:
		return nil, xerrors.Errorf("Record status is 'undefined'")
	}

	res := v.Get()
	if res == nil {
		return nil, nil
	}
	var result []any
	if err := v.AssignTo(&result); err != nil {
		return nil, xerrors.Errorf("failed to assign Record to an untyped slice: %w", err)
	}
	return result, nil
}

func unmarshalJSON(v *pgtype.JSON) (any, error) {
	switch v.Status {
	case pgtype.Null:
		return nil, nil
	case pgtype.Undefined:
		return nil, xerrors.Errorf("JSON status is 'undefined'")
	}

	result, err := jsonx.NewValueDecoder(jsonx.NewDefaultDecoder(bytes.NewReader(v.Bytes))).Decode()
	if err != nil {
		return nil, xerrors.Errorf("failed to decode a serialized JSON: %w", err)
	}
	return result, nil
}

func unmarshalCIDR(v *pgtype.CIDR) (any, error) {
	switch v.Status {
	case pgtype.Null:
		return nil, nil
	case pgtype.Undefined:
		return nil, xerrors.Errorf("CIDR status is 'undefined'")
	}

	return unmarshalIPNet(v.IPNet)
}

func unmarshalIPNet(v *net.IPNet) (any, error) {
	if v == nil {
		return nil, nil
	}
	return v.String(), nil
}

func unmarshalDaterange(v *pgtype.Daterange) (any, error) {
	switch v.Status {
	case pgtype.Null:
		return nil, nil
	case pgtype.Undefined:
		return nil, xerrors.Errorf("Daterange status is 'undefined'")
	}

	dr := make([]time.Time, 2)
	dr[0] = v.Lower.Time
	dr[1] = v.Upper.Time
	return DaterangeToString(dr), nil
}

func ensureJSONMarshallable(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	return castx.ToJSONMarshallableE(v)
}

func expectedStringCast(value any) (any, error) {
	var result any
	var err error

	switch v := value.(type) {
	case *pgtype.GenericText:
		result, err = strict.ExpectedSQL[*pgtype.GenericText](v, castx.ToStringE)
	case *pgtype.Text:
		result, err = strict.ExpectedSQL[*pgtype.Text](v, castx.ToStringE)
	case *pgtype.Varchar:
		result, err = strict.ExpectedSQL[*pgtype.Varchar](v, castx.ToStringE)
	case *pgtype.Interval:
		// funny that Interval is schema.String, while Tstzrange and Daterange are schema.Any
		// Note that none of them are schema.Interval
		result, err = strict.ExpectedSQL[*pgtype.Interval](v, castx.ToStringE)
	case string:
		// this should never appear, but has been present since https://github.com/transferia/transferia/review/757334/files/2#file-/trunk/arcadia/transfer_manager/go/pkg/storage/pg/scanner.go:R24
		result, err = v, nil
	default:
		result, err = strict.UnexpectedSQL(v, castx.ToStringE)
	}

	if err != nil {
		return nil, xerrors.Errorf("failed to cast %T to string: %w", value, err)
	}
	return result, nil
}
