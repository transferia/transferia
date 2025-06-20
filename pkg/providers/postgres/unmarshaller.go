//go:build !disable_postgres_provider

package postgres

import (
	"time"

	"github.com/gofrs/uuid"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util/xlocale"
	"go.ytsaurus.tech/library/go/core/log"
)

type UnmarshallerData struct {
	isHomo   bool
	location *time.Location
}

func MakeUnmarshallerData(isHomo bool, conn *pgx.Conn) UnmarshallerData {
	marshalTimezone := conn.PgConn().ParameterStatus(TimeZoneParameterStatusKey)
	if isHomo {
		// use UTC in homo transfers, otherwise timestamps WITHOUT time zone cannot be inserted with COPY
		marshalTimezone = time.UTC.String()
	}
	location, err := xlocale.Load(marshalTimezone)
	if err != nil {
		logger.Log.Warn("failed to parse time zone", log.String("timezone", marshalTimezone), log.Error(err))
		location = time.UTC
	}

	return UnmarshallerData{
		isHomo:   isHomo,
		location: location,
	}
}

// Unmarshaller converts data from the PostgreSQL format to the Data Transfer format. The format differs for homogenous and heterogenous transfers, but the same unmarshaller is used in both cases.
type Unmarshaller struct {
	castData  *UnmarshallerData
	connInfo  *pgtype.ConnInfo
	schema    *abstract.ColSchema
	fieldDesc *pgproto3.FieldDescription

	decoder any
}

func NewUnmarshaller(castData *UnmarshallerData, connInfo *pgtype.ConnInfo, schema *abstract.ColSchema, fieldDesc *pgproto3.FieldDescription) (*Unmarshaller, error) {
	result := &Unmarshaller{
		castData:  castData,
		connInfo:  connInfo,
		schema:    schema,
		fieldDesc: fieldDesc,

		decoder: nil,
	}
	return result, nil
}

// Cast consumes raw SELECT output and produces a valid ChangeItem.Value
func (c *Unmarshaller) Cast(input []byte) (any, error) {
	if c.decoder == nil {
		if err := c.reconstructDecoder(); err != nil {
			return nil, xerrors.Errorf("failed to (re)construct decoder: %w", err)
		}
	}

	switch c.fieldDesc.Format {
	case pgtype.BinaryFormatCode:
		d, ok := c.decoder.(pgtype.BinaryDecoder)
		if !ok {
			return nil, abstract.NewFatalError(xerrors.Errorf("BinaryDecoder is requested, but the actual decoder is of type %T and does not implement BinaryDecoder", c.decoder))
		}
		if err := d.DecodeBinary(c.connInfo, input); err != nil {
			return nil, xerrors.Errorf("failed to decode a value in binary format: %w", err)
		}
	case pgtype.TextFormatCode:
		d, ok := c.decoder.(pgtype.TextDecoder)
		if !ok {
			return nil, abstract.NewFatalError(xerrors.Errorf("TextDecoder is requested, but the actual decoder is of type %T and does not implement TextDecoder", c.decoder))
		}
		if err := d.DecodeText(c.connInfo, input); err != nil {
			return nil, xerrors.Errorf("failed to decode a value in text format: %w", err)
		}
	default:
		return nil, abstract.NewFatalError(xerrors.Errorf("unknown decoder format code in cast: %d", c.fieldDesc.Format))
	}

	var result any = nil
	if c.castData.isHomo {
		result = unmarshalFieldHomo(c.decoder, c.schema)
		c.decoder = nil
	} else {
		r, err := unmarshalFieldHetero(c.decoder, c.schema, c.connInfo)
		if err != nil {
			return nil, xerrors.Errorf("failed to unmarshal a value: %w", err)
		}
		result = r
		// in hetero case, marshalField returns a new value, never a decoder itself, so the decoder can be reused
	}
	return result, nil
}

func (c *Unmarshaller) reconstructDecoder() error {
	var proposedDecoder any = nil
	if dt, ok := c.connInfo.DataTypeForOID(c.fieldDesc.DataTypeOID); ok {
		switch dt.OID {
		case pgtype.TimestampOID:
			proposedDecoder = NewTimestamp(c.castData.location)
		case pgtype.TimestamptzOID:
			proposedDecoder = NewTimestamptz()
		case pgtype.DateOID:
			proposedDecoder = NewDate()
		default:
			proposedDecoder = pgtype.NewValue(dt.Value)
		}
	} else {
		logger.Log.Warn("detected a type with unknown OID. It will be parsed without proper type conversions", log.UInt32("table_oid", c.fieldDesc.TableOID), log.UInt16("table_attribute_number", c.fieldDesc.TableAttributeNumber), log.UInt32("type_oid", c.fieldDesc.DataTypeOID), log.String("type_name", string(c.fieldDesc.Name)), log.Int16("format_code", c.fieldDesc.Format))
	}

	switch c.fieldDesc.Format {
	case pgtype.BinaryFormatCode:
		if binaryDecoder, casts := proposedDecoder.(pgtype.BinaryDecoder); casts {
			c.decoder = binaryDecoder
		} else {
			// note this may happen both for a type which does not support binary decoding OR for a type with unknown OID, for which no decoder is proposed
			c.decoder = &pgtype.GenericBinary{}
		}
	case pgtype.TextFormatCode:
		if textDecoder, casts := proposedDecoder.(pgtype.TextDecoder); casts {
			c.decoder = textDecoder
		} else {
			// note this may happen both for a type which does not support text decoding OR for a type with unknown OID, for which no decoder is proposed
			c.decoder = &pgtype.GenericText{}
		}
	default:
		return abstract.NewFatalError(xerrors.Errorf("unknown decoder format code %d", c.fieldDesc.Format))
	}

	return nil
}

func unmarshalFieldHomo(val any, schema *abstract.ColSchema) any {
	switch v := val.(type) {
	case [16]byte:
		if schema.OriginalType != "pg:uuid" {
			return val
		}
		vUUID := pgtype.UUID{}
		if err := vUUID.Set(v); err != nil {
			logger.Log.Info("failed to marshal (call Set) into UUID", log.Error(err))
			return uuid.FromBytesOrNil(v[:16]).String()
		}
		result, err := vUUID.Value()
		if err != nil {
			logger.Log.Info("failed to marshal (call Value) into UUID", log.Error(err))
			return uuid.FromBytesOrNil(v[:16]).String()
		}
		return result
	case abstract.HomoValuer:
		return v.HomoValue()
	default:
		return val
	}
}
