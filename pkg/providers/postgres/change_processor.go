//go:build !disable_postgres_provider

package postgres

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/spf13/cast"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/castx"
	"github.com/transferia/transferia/pkg/util/jsonx"
	"github.com/transferia/transferia/pkg/util/set"
	"github.com/transferia/transferia/pkg/util/strict"
	"github.com/transferia/transferia/pkg/util/xlocale"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
)

type changeProcessor struct {
	connInfo pgtype.ConnInfo

	schemaTimestamp time.Time
	schemasToEmit   abstract.DBSchema
	// fastSchemas contain COPIES of the original schemas
	fastSchemas map[abstract.TableID]abstract.FastTableSchema

	config   *PgSource
	location *time.Location
}

func defaultChangeProcessor() *changeProcessor {
	return &changeProcessor{
		connInfo: *pgtype.NewConnInfo(),
		location: time.UTC,

		schemaTimestamp: time.Now(),
		schemasToEmit:   abstract.DBSchema{},
		fastSchemas:     fastSchemasFromDBSchema(abstract.DBSchema{}),

		config: new(PgSource),
	}
}

func newChangeProcessor(
	conn *pgx.Conn,
	dbSchema abstract.DBSchema,
	schemaTimestamp time.Time,
	config *PgSource,
) (*changeProcessor, error) {
	connTimezone := conn.PgConn().ParameterStatus(TimeZoneParameterStatusKey)
	if config.IsHomo {
		// use UTC in homo transfers, otherwise timestamps WITHOUT time zone cannot be inserted with COPY
		connTimezone = time.UTC.String()
	}
	location, err := xlocale.Load(connTimezone)
	if err != nil {
		logger.Log.Warn("failed to parse time zone", log.String("timezone", connTimezone), log.Error(err))
		location = time.UTC
	}

	return &changeProcessor{
		connInfo: *conn.ConnInfo(),
		location: location,

		schemasToEmit:   dbSchema,
		schemaTimestamp: schemaTimestamp,
		fastSchemas:     fastSchemasFromDBSchema(dbSchema),

		config: config,
	}, nil
}

func fastSchemasFromDBSchema(dbSchema abstract.DBSchema) map[abstract.TableID]abstract.FastTableSchema {
	result := make(map[abstract.TableID]abstract.FastTableSchema)
	for t, tColumns := range dbSchema {
		result[t] = abstract.MakeFastTableSchema(tColumns.Copy().Columns())
	}
	return result
}

func (c *changeProcessor) hasSchemaForTable(id abstract.TableID) bool {
	_, ok := c.schemasToEmit[id]
	return ok
}

func (c *changeProcessor) fixupChange(
	change *abstract.ChangeItem,
	columnTypeOIDs []pgtype.OID,
	oldKeyTypeOIDs []pgtype.OID,
	counter int,
	lsn pglogrepl.LSN,
) error {
	change.Counter = counter
	change.LSN = uint64(lsn)

	emissionSchema, ok := c.schemasToEmit[change.TableID()]
	if !ok {
		return makeChangeItemError(fmt.Sprintf("table schema for table %s is unknown; please check if the user has the SELECT permission for this table", change.TableID().Fqtn()), change)
	}
	parsingSchema, ok := c.fastSchemas[change.TableID()]
	if !ok {
		return abstract.NewFatalError(makeChangeItemError(fmt.Sprintf("fast table schema is missing for table %s", change.TableID().Fqtn()), change))
	}
	change.SetTableSchema(emissionSchema)

	if len(change.ColumnNames) != len(change.ColumnValues) {
		msg := fmt.Sprintf("len(ColumnNames) != len(ColumnValues) (%d != %d)", len(change.ColumnNames), len(change.ColumnValues))
		return makeChangeItemError(msg, change)
	}
	if len(change.OldKeys.KeyNames) != len(change.OldKeys.KeyValues) {
		msg := fmt.Sprintf("len(OldKeys.ColumnNames) != len(OldKeys.ColumnValues) (%d != %d)", len(change.OldKeys.KeyNames), len(change.OldKeys.KeyValues))
		return makeChangeItemError(msg, change)
	}

	errs := util.NewErrs()

	skipCols := set.New[string]()
	for i, name := range change.ColumnNames {
		colSchema, ok := parsingSchema[abstract.ColumnName(name)]
		if !ok {
			// schema timestamp is greater than change item, so it fresher that change
			// when we miss column in cache and schema timestamp is greater than change item
			// and still miss a column we faced cache-miss for column, we can't proceed further, and must skip such columns
			if c.schemaTimestamp.Sub(time.Unix(0, int64(change.CommitTime))) > 0 {
				skipCols.Add(name)
				continue
			}

			errs = util.AppendErr(errs, xerrors.Errorf(`Unknown column "%s"; table schema has probably changed`, name))
		}
		var err error
		change.ColumnValues[i], err = c.restoreType(change.ColumnValues[i], columnTypeOIDs[i], &colSchema)
		if err != nil {
			errs = util.AppendErr(errs, xerrors.Errorf("Can't cast value '%v' for column '%v': %w", change.ColumnValues[i], name, err))
		}
	}
	if !skipCols.Empty() {
		logger.Log.Info("skip change item columns", log.Any("skipCols", skipCols.Slice()), log.Any("table", change.TableID()))
		change.RemoveColumns(skipCols.Slice()...)
	}

	for i, name := range change.OldKeys.KeyNames {
		colSchema, ok := parsingSchema[abstract.ColumnName(name)]
		if !ok {
			errs = util.AppendErr(errs, xerrors.Errorf(`Unknown old key column "%s"; table schema has probably changed`, name))
			continue
		}
		var err error
		change.OldKeys.KeyValues[i], err = c.restoreType(change.OldKeys.KeyValues[i], oldKeyTypeOIDs[i], &colSchema)
		if err != nil {
			errs = util.AppendErr(errs, xerrors.Errorf("Can't cast value '%v' for column '%v': %w", change.ColumnValues[i], name, err))
		}
	}

	if len(errs) > 0 {
		return makeChangeItemError(errs.String(), change)
	}

	c.filterUserTypes(parsingSchema, change)

	return nil
}

func makeChangeItemError(message string, change *abstract.ChangeItem) error {
	changeJSON, _ := json.Marshal(change)
	return xerrors.Errorf("Cannot process change item: %s; item: %s", message, truncate(string(changeJSON), 5000))
}

func (c *changeProcessor) filterUserTypes(types abstract.FastTableSchema, change *abstract.ChangeItem) {
	if !c.config.IgnoreUserTypes {
		return
	}
	if len(change.ColumnNames) == 0 {
		return
	}
	for i, col := range change.ColumnNames {
		typ := types[abstract.ColumnName(col)]
		if IsUserDefinedType(&typ) {
			change.ColumnValues[i] = nil
		}
	}
}

func (c *changeProcessor) restoreType(value any, oid pgtype.OID, colSchema *abstract.ColSchema) (any, error) {
	var result any
	var err error

	// in the switch below, the usage of `strict.UnexpectedSQL` indicates an unexpected or even impossible situation.
	// However, in order for Data Transfer to remain resilient, "unexpected" casts must exist
	switch schema.Type(colSchema.DataType) {
	case schema.TypeInt64:
		result, err = strict.ExpectedSQL[json.Number](value, cast.ToInt64E)
	case schema.TypeInt32:
		result, err = strict.ExpectedSQL[json.Number](value, cast.ToInt32E)
	case schema.TypeInt16:
		result, err = strict.ExpectedSQL[json.Number](value, cast.ToInt16E)
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
		result, err = strict.ExpectedSQL[json.Number](value, castx.ToJSONNumberE)
	case schema.TypeBytes:
		result, err = strict.ExpectedSQL[string](value, unmarshalHexStringBytes)
	case schema.TypeBoolean:
		result, err = strict.ExpectedSQL[bool](value, cast.ToBoolE)
	case schema.TypeDate:
		result, err = strict.ExpectedSQL[string](value, temporalsUnmarshallerFromDecoder(NewDate(), &c.connInfo, c.config.IsHomo))
	case schema.TypeDatetime:
		switch ClearOriginalType(colSchema.OriginalType) {
		case "TIMESTAMP WITH TIME ZONE":
			result, err = strict.ExpectedSQL[string](value, temporalsUnmarshallerFromDecoder(NewTimestamptz(), &c.connInfo, c.config.IsHomo))
		case "TIMESTAMP WITHOUT TIME ZONE":
			result, err = strict.ExpectedSQL[string](value, temporalsUnmarshallerFromDecoder(NewTimestamp(c.location), &c.connInfo, c.config.IsHomo))
		default:
			result, err = strict.UnexpectedSQL(value, cast.ToTimeE)
		}
	case schema.TypeTimestamp:
		switch ClearOriginalType(colSchema.OriginalType) {
		case "TIMESTAMP WITH TIME ZONE":
			result, err = strict.UnexpectedSQL(value, temporalsUnmarshallerFromDecoder(NewTimestamptz(), &c.connInfo, c.config.IsHomo))
		case "TIMESTAMP WITHOUT TIME ZONE":
			result, err = strict.UnexpectedSQL(value, temporalsUnmarshallerFromDecoder(NewTimestamp(c.location), &c.connInfo, c.config.IsHomo))
		default:
			result, err = strict.UnexpectedSQL(value, cast.ToTimeE)
		}
	case schema.TypeInterval:
		result, err = strict.UnexpectedSQL(value, cast.ToDurationE)
	case schema.TypeString:
		switch ClearOriginalType(colSchema.OriginalType) {
		case "TEXT", "CHARACTER VARYING":
			result, err = strict.ExpectedSQL[string](value, castx.ToStringE)
		case "TIME WITH TIME ZONE", "TIME WITHOUT TIME ZONE":
			result, err = strict.ExpectedSQL[string](value, castx.ToStringE)
		case "INTERVAL":
			result, err = strict.ExpectedSQL[string](value, castx.ToStringE)
		case "MONEY":
			result, err = strict.ExpectedSQL[string](value, castx.ToStringE)
		default:
			result, err = strict.UnexpectedSQL(value, castx.ToStringE)
		}
	case schema.TypeAny:
		result, err = expectedAnyCastReplication(value, oid, colSchema, &c.connInfo, c.config.IsHomo)
	default:
		return nil, abstract.NewFatalError(xerrors.Errorf("unexpected target type %s (original type %q, value of type %T), unmarshalling is not implemented", colSchema.DataType, colSchema.OriginalType, value))
	}

	if err != nil {
		return nil, abstract.NewStrictifyError(colSchema, schema.Type(colSchema.DataType), err)
	}
	return result, nil
}

func (c *changeProcessor) resolveParentTable(ctx context.Context, connPool *pgxpool.Pool, id abstract.TableID) (res abstract.TableID, err error) {
	conn, err := connPool.Acquire(ctx)
	if err != nil {
		return res, xerrors.Errorf("unable to acquire conn: %w", err)
	}
	defer conn.Release()
	tinfo, err := newTableInformationSchema(ctx, conn, abstract.TableDescription{
		Name:   id.Name,
		Schema: id.Namespace,
		Filter: "",
		EtaRow: 0,
		Offset: 0,
	})
	if err != nil {
		return res, xerrors.Errorf("unable to load table information: %w", err)
	}
	if tinfo.ParentTable != "" {
		tid, err := abstract.NewTableIDFromStringPg(tinfo.ParentTable, true)
		if err != nil {
			return res, xerrors.Errorf("unable to parse parent table name: %w", err)
		}
		return *tid, nil
	}
	return id, nil
}

func unmarshalHexStringBytes(v any) ([]byte, error) {
	var hexString string
	switch downcastedHexString := v.(type) {
	case string:
		hexString = downcastedHexString
	default:
		convertedHexString, err := castx.ToStringE(downcastedHexString)
		if err != nil {
			return nil, xerrors.Errorf("failed to cast %T to string: %w", downcastedHexString, err)
		}
		hexString = convertedHexString
	}

	result, err := hex.DecodeString(hexString)
	if err != nil {
		return nil, xerrors.Errorf("failed to decode BYTEA from a string in hex representation: %w", err)
	}
	return result, nil
}

type TextDecoderAndValuerWithHomo interface {
	pgtype.TextDecoder
	driver.Valuer
	abstract.HomoValuer
}

func temporalsUnmarshallerFromDecoder(decoder TextDecoderAndValuerWithHomo, ci *pgtype.ConnInfo, isHomo bool) func(v any) (any, error) {
	return func(v any) (any, error) {
		if v == nil {
			return nil, nil
		}
		var vS string
		switch vv := v.(type) {
		case string:
			vS = vv
		default:
			vvv, err := castx.ToStringE(vv)
			if err != nil {
				return time.UnixMicro(0), xerrors.Errorf("failed to cast %T to string: %w", vv, err)
			}
			vS = vvv
		}

		if err := decoder.DecodeText(ci, []byte(vS)); err != nil {
			return time.UnixMicro(0), xerrors.Errorf("failed to decode a value with %T: %w", decoder, err)
		}

		if isHomo {
			return decoder.HomoValue(), nil
		}

		result, err := decoder.Value()
		if err != nil {
			return time.UnixMicro(0), xerrors.Errorf("failed to obtain a value after decoding from %T: %w", decoder, err)
		}
		resultT, ok := result.(time.Time)
		if !ok {
			return time.UnixMicro(0), xerrors.Errorf("a value of type %T obtained from a decoder %T is not an instance of time.Time", result, decoder)
		}
		return resultT, nil
	}
}

func expectedAnyCastReplication(value any, oid pgtype.OID, colSchema *abstract.ColSchema, connInfo *pgtype.ConnInfo, isHomo bool) (any, error) {
	if value == nil {
		return nil, nil
	}

	var result any
	var err error
	switch v := value.(type) {
	case string:
		if !isHomo {
			unmarshalled, successful, unmarshalErr := tryUnmarshalComplexType(v, oid, connInfo)
			if unmarshalErr != nil {
				logger.Log.Warn("failed to unpack complex types", log.Error(unmarshalErr))
			} else {
				if successful {
					result, err = unmarshalled, nil
					break
				}
			}
		}
		switch ClearOriginalType(colSchema.OriginalType) {
		case "LSEG":
			result, err = unmarshalLSeg(v)
		case "JSON", "JSONB":
			result, err = unmarshalAnyJSON(v)
		case "HSTORE":
			result, err = HstoreToMap(v)
		default:
			result, err = v, nil
		}
	default:
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

// Attempts to convert a string representation of a complex type (an array or a
// composite type) into a proper Go object:
//   - []interface{} for slices;
//   - string for composite types (map[string]interface{} would be better, but we
//     use string for the backward compatibility).
func tryUnmarshalComplexType(value string, oid pgtype.OID, connInfo *pgtype.ConnInfo) (result any, isUnpacked bool, err error) {
	dt, ok := connInfo.DataTypeForOID(uint32(oid))
	if !ok {
		return nil, false, xerrors.Errorf("failed to get PostgreSQL data type for OID %d from connection information: %w", oid, err)
	}

	switch concreteValue := dt.Value.(type) {
	case *GenericArray:
		v := concreteValue.NewTypeValueImpl()
		if err := v.DecodeText(connInfo, []byte(value)); err != nil {
			return nil, false, xerrors.Errorf("failed to decode generic array (OID %d) from text: %w", oid, err)
		}
		result, err := v.ExtractValue(connInfo)
		if err != nil {
			return nil, false, xerrors.Errorf("failed to extract generic array (OID %d) value: %w", oid, err)
		}
		return result, true, nil
	case *pgtype.CompositeType:
		// In case of a composite type, we could just return value string
		// which we have received from wal2json, as we did before using
		// pgtype.CompositeType. But instead, we decode the composite type and
		// encode it back to text.  Why? Unfortunately, in PostgreSQL record
		// (aka composite) serialization format is a bit loose: there are two
		// ways of escaping a double quote: \" (backslash + dquote) and "" (two
		// dquotes).  PostgreSQL server uses the latter when serializing a
		// record, while pgtype uses the former. We use pgtype.CompositeType to
		// make the output uniform both on snapshot (where pgtype.CompositeType
		// is also used to represent composite types, and thus \" escaping form
		// is used) and on replication.
		compositeValue := concreteValue.NewTypeValue().(*pgtype.CompositeType)
		if err := compositeValue.DecodeText(connInfo, []byte(value)); err != nil {
			return nil, false, xerrors.Errorf("failed to decode composite type (OID %d %q) from text: %w", oid, concreteValue.TypeName(), err)
		}
		buf, err := compositeValue.EncodeText(connInfo, nil)
		if err != nil {
			return nil, false, xerrors.Errorf("failed to re-encode composite type (OID %d %q) into text: %w", oid, concreteValue.TypeName(), err)
		}
		return string(buf), true, nil
	default:
		return nil, false, nil
	}
}

func unmarshalLSeg(v string) (any, error) {
	decoder := new(pgtype.Lseg)
	if err := decoder.DecodeText(nil, []byte(v)); err != nil {
		return "", xerrors.Errorf("unable to decode lseg: %w", err)
	}
	recodedV, err := pgtype.EncodeValueText(decoder)
	if err != nil {
		return "", xerrors.Errorf("unable to re-encode lseg to text: %w", err)
	}
	return recodedV, nil
}

func unmarshalAnyJSON(v string) (any, error) {
	decoder := jsonx.NewValueDecoder(jsonx.NewDefaultDecoder(bytes.NewReader([]byte(v))))
	result, err := decoder.Decode()
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal JSON: %w", err)
	}
	return result, nil
}
