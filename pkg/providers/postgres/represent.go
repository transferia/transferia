package postgres

import (
	"bytes"
	sql_driver "database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgtype"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func appendEscapedSingleQuotesToWriter(w *bytes.Buffer, s string) {
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			if i > start {
				_, _ = w.WriteString(s[start:i])
			}
			_, _ = w.WriteString("''")
			start = i + 1
		}
	}
	if start < len(s) {
		_, _ = w.WriteString(s[start:])
	}
}

func appendEscapedSingleQuotesBytesToWriter(w *bytes.Buffer, b []byte) {
	start := 0
	for i := 0; i < len(b); i++ {
		if b[i] == '\'' {
			if i > start {
				_, _ = w.Write(b[start:i])
			}
			_, _ = w.WriteString("''")
			start = i + 1
		}
	}
	if start < len(b) {
		_, _ = w.Write(b[start:])
	}
}

func appendEscapedBackslashesAndSingleQuotesToWriter(w *bytes.Buffer, s string) {
	start := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			if i > start {
				_, _ = w.WriteString(s[start:i])
			}
			_, _ = w.WriteString("\\\\")
			start = i + 1
		case '\'':
			if i > start {
				_, _ = w.WriteString(s[start:i])
			}
			_, _ = w.WriteString("''")
			start = i + 1
		}
	}
	if start < len(s) {
		_, _ = w.WriteString(s[start:])
	}
}

func writeQuotedInt64(w *bytes.Buffer, v int64) {
	var tmp [24]byte
	out := tmp[:0]
	out = append(out, '\'')
	out = strconv.AppendInt(out, v, 10)
	out = append(out, '\'')
	_, _ = w.Write(out)
}

func writeQuotedUint64(w *bytes.Buffer, v uint64) {
	var tmp [24]byte
	out := tmp[:0]
	out = append(out, '\'')
	out = strconv.AppendUint(out, v, 10)
	out = append(out, '\'')
	_, _ = w.Write(out)
}

func writeQuotedFloat64(w *bytes.Buffer, v float64, format byte, prec int) {
	var tmp [64]byte
	out := tmp[:0]
	out = append(out, '\'')
	out = strconv.AppendFloat(out, v, format, prec, 64)
	out = append(out, '\'')
	_, _ = w.Write(out)
}

func appendRepresentTimeToWriter(w *bytes.Buffer, v time.Time, colSchema abstract.ColSchema) {
	var (
		t      time.Time
		layout string
	)

	switch colSchema.DataType {
	case ytschema.TypeDate.String():
		t = v.UTC()
		layout = PgDateFormat
	case ytschema.TypeDatetime.String():
		t = v.UTC()
		layout = PgDatetimeFormat
	case ytschema.TypeTimestamp.String():
		// note `v` is not converted to UTC. As a result, when the target field is TIMESTAMP WITHOUT TIME ZONE, the incoming value of the timestamp will be preserved, but its timezone will be (automatically) set to the local time zone of the target database
		t = v
		layout = PgTimestampFormat
	default:
		t = v.UTC()
		layout = PgTimestampFormat
	}

	var buf [64]byte
	formatted := t.AppendFormat(buf[:0], layout)

	_ = w.WriteByte('\'')
	if len(formatted) > 0 && formatted[0] == '-' {
		_, _ = w.Write(formatted[1:])
		_, _ = w.WriteString(" BC")
	} else {
		_, _ = w.Write(formatted)
	}
	_ = w.WriteByte('\'')
}

// timetzBinaryToTime converts a 12-byte binary timetz value into time.Time.
func timetzBinaryToTime(data []byte) (time.Time, error) {
	if len(data) != 12 {
		return time.Time{}, fmt.Errorf("timetz: expected 12 bytes, got %d", len(data))
	}

	// First 8 bytes — int64 big-endian: microseconds since midnight
	usec := int64(binary.BigEndian.Uint64(data[0:8]))

	// Next 4 bytes — int32 big-endian: timezone offset in seconds
	zoneSec := int32(binary.BigEndian.Uint32(data[8:12]))

	// Build time.Time: base date 2000-01-01 + time-of-day in UTC+offset zone
	hours := usec / 3_600_000_000
	usec -= hours * 3_600_000_000

	minutes := usec / 60_000_000
	usec -= minutes * 60_000_000

	seconds := usec / 1_000_000
	usec -= seconds * 1_000_000

	ns := usec * 1000

	loc := time.FixedZone("timetz", int(zoneSec))
	t := time.Date(2000, 1, 1, int(hours), int(minutes), int(seconds), int(ns), loc)
	return t, nil
}

func moneyFromBinary(src []byte) string {
	if src == nil {
		return "NULL"
	}
	cents := int64(binary.BigEndian.Uint64(src))
	return fmt.Sprintf("%.2f", float64(cents)/100.0)
}

func recursiveRepresentToWriter(w *bytes.Buffer, val interface{}, colSchema abstract.ColSchema) error {
	if val == nil {
		_, _ = w.WriteString("null")
		return nil
	}

	if IsPgTypeTimeWithTimeZone(colSchema.OriginalType) {
		var timeBytes []byte = nil
		switch v := val.(type) {
		case *pgtype.GenericBinary:
			timeBytes = v.Bytes
		case []byte:
			timeBytes = v
		case string:
			_ = w.WriteByte('\'')
			appendEscapedSingleQuotesToWriter(w, v)
			_ = w.WriteByte('\'')
			return nil
		default:
			return xerrors.Errorf("unknown type for IsPgTypeTimeWithTimeZone: %T", v)
		}

		timeTime, err := timetzBinaryToTime(timeBytes)
		if err != nil {
			return xerrors.Errorf("timetzBinaryToTime: %w", err)
		}
		appendRepresentTimeToWriter(w, timeTime, colSchema)
		return nil
	}

	if colSchema.OriginalType == "pg:money" {
		if v, ok := val.(*pgtype.GenericBinary); ok {
			if len(v.Bytes) == 0 {
				_, _ = w.WriteString("null")
				return nil
			} else {
				pgMoneyVal := moneyFromBinary(v.Bytes)
				_ = w.WriteByte('\'')
				appendEscapedBackslashesAndSingleQuotesToWriter(w, pgMoneyVal)
				_ = w.WriteByte('\'')
			}
			return nil
		}
	}

	if IsUserDefinedType(&colSchema) {
		if _, ok := IsPgUserDefinedEnum(colSchema.OriginalType); ok {
			if v, ok := val.(*pgtype.GenericBinary); ok {
				if len(v.Bytes) == 0 {
					_, _ = w.WriteString("null")
					return nil
				} else {
					_, _ = w.WriteString(fmt.Sprintf("'%v'", string(v.Bytes)))
					return nil
				}
			} else {
				_, _ = w.WriteString(fmt.Sprintf("'%v'", val))
				return nil
			}
		} else if colSchema.OriginalType == PgUserDefinedHStore {
			if hstoreObj, ok := val.(*pgtype.Hstore); ok {
				hstoreStr, err := hstoreObj.EncodeText(nil, make([]byte, 0))
				if err != nil {
					return xerrors.Errorf("unable to encode hstore: %w", err)
				}
				if len(hstoreStr) == 0 {
					_, _ = w.WriteString("null")
					return nil
				} else {
					_ = w.WriteByte('\'')
					appendEscapedSingleQuotesToWriter(w, string(hstoreStr))
					_ = w.WriteByte('\'')
					return nil
				}
			} else if hstoreStrObj, ok := val.(string); ok {
				if len(hstoreStrObj) == 0 {
					_, _ = w.WriteString("null")
					return nil
				} else {
					_ = w.WriteByte('\'')
					appendEscapedSingleQuotesToWriter(w, hstoreStrObj)
					_ = w.WriteByte('\'')
					return nil
				}
			} else {
				s, _ := json.Marshal(val)
				h, _ := JSONToHstore(string(s))
				_ = w.WriteByte('\'')
				appendEscapedSingleQuotesToWriter(w, h)
				_ = w.WriteByte('\'')
				return nil
			}
		} else if colSchema.OriginalType == PgUserDefinedCIText {
			valStr := fmt.Sprintf("'%v'", val)
			if len(valStr) == 0 {
				_, _ = w.WriteString("null")
				return nil
			} else {
				_, _ = w.WriteString(valStr)
				return nil
			}
		} else if IsUserDefinedType(&colSchema) {
			if currCompositeType, ok := val.(*pgtype.CompositeType); ok {
				buf, err := currCompositeType.EncodeText(nil, nil)
				if err != nil {
					return xerrors.Errorf("unable to encode composite type: %w", err)
				}
				bufStr := string(buf)
				if len(bufStr) == 0 {
					_, _ = w.WriteString("null")
					return nil
				} else {
					_, _ = w.WriteString(fmt.Sprintf("'%s'", bufStr))
					return nil
				}
			} else if compositeTypeString, ok := val.(string); ok {
				if len(compositeTypeString) == 0 {
					_, _ = w.WriteString("null")
					return nil
				} else {
					_ = w.WriteByte('\'')
					appendEscapedBackslashesAndSingleQuotesToWriter(w, compositeTypeString)
					_ = w.WriteByte('\'')
					return nil
				}
			} else {
				_, _ = w.WriteString(fmt.Sprintf("'%v'", val))
				return nil
			}
		} else {
			return xerrors.Errorf("unsupported USER-DEFINED type: '%s'", colSchema.OriginalType)
		}
	}

	if v, ok := val.(*pgtype.CIDR); ok {
		// pgtype.CIDR does not implement driver.Valuer, but its implementation is intended
		val = (*pgtype.Inet)(v)
	}

	if v, ok := val.(sql_driver.Valuer); ok {
		vv, _ := v.Value()

		if strings.HasPrefix(colSchema.OriginalType, "pg:time") &&
			!strings.HasPrefix(colSchema.OriginalType, "pg:timestamp") {
			// by default Value of time always returns as array of bytes which can not be processed in plain insert
			// however if we cast decoder to pgtype.Time while unmarshalling it will lead to errors in tests because
			// pgtype.Time doesn't store the precision and always uses the maximum(6)
			if vvv, ok := vv.([]byte); ok && vv != nil {
				coder := new(pgtype.Time)

				// we only use binary->binary (de)serialization in homogeneous pg->pg
				if err := coder.DecodeBinary(nil, vvv); err == nil {
					//nolint:descriptiveerrors
					return recursiveRepresentToWriter(w, coder, colSchema)
				}
			}
		}

		if strings.HasPrefix(colSchema.OriginalType, "pg:json") {
			if vvv, ok := vv.(string); ok && vv != nil {
				// Valuer may start with special character, which we must erase
				// If JSON not started with { or ] we should erase first byte
				if len(vvv) > 0 && vvv[0] != '{' && vvv[0] != '[' {
					err := recursiveRepresentToWriter(w, vvv[1:], colSchema)
					if err != nil {
						return xerrors.Errorf("unable to represent value from pg:json/pg:jsonb: %w", err)
					}
					return nil
				}
			}
			if vvv, ok := vv.([]uint8); ok && vv != nil {
				_ = w.WriteByte('\'')
				appendEscapedSingleQuotesBytesToWriter(w, vvv)
				_ = w.WriteByte('\'')
				return nil
			}
		}
		//nolint:descriptiveerrors
		return recursiveRepresentToWriter(w, vv, colSchema)
	}

	if colSchema.OriginalType == "" && colSchema.DataType == ytschema.TypeAny.String() { // no-homo json
		s, _ := json.Marshal(val)
		_ = w.WriteByte('\'')
		appendEscapedSingleQuotesBytesToWriter(w, s)
		_ = w.WriteByte('\'')
		return nil
	}
	if colSchema.OriginalType == "pg:json" || colSchema.OriginalType == "pg:jsonb" {
		s, _ := json.Marshal(val)
		_ = w.WriteByte('\'')
		appendEscapedSingleQuotesBytesToWriter(w, s)
		_ = w.WriteByte('\'')
		return nil
	}

	switch v := val.(type) {
	case string:
		switch colSchema.DataType {
		case ytschema.TypeBytes.String():
			_ = w.WriteByte('\'')
			appendEscapedBackslashesAndSingleQuotesToWriter(w, v)
			_ = w.WriteByte('\'')
			return nil
		default:
			_ = w.WriteByte('\'')
			appendEscapedSingleQuotesToWriter(w, v)
			_ = w.WriteByte('\'')
			return nil
		}
	case *time.Time:
		appendRepresentTimeToWriter(w, *v, colSchema)
		return nil
	case time.Time:
		appendRepresentTimeToWriter(w, v, colSchema)
		return nil
	case []byte:
		_, _ = w.WriteString("'\\x")
		_, _ = w.WriteString(hex.EncodeToString(v))
		_ = w.WriteByte('\'')
		return nil
	case int:
		writeQuotedInt64(w, int64(v))
		return nil
	case int32:
		writeQuotedInt64(w, int64(v))
		return nil
	case int64:
		writeQuotedInt64(w, v)
		return nil
	case uint32:
		writeQuotedUint64(w, uint64(v))
		return nil
	case uint64:
		writeQuotedUint64(w, v)
		return nil
	case float64:
		if strings.HasPrefix(colSchema.DataType, "uint") {
			writeQuotedUint64(w, uint64(v))
			return nil
		}
		if strings.HasPrefix(colSchema.DataType, "int") || colSchema.OriginalType == "pg:bigint" {
			writeQuotedInt64(w, int64(v))
			return nil
		}
		if strings.Contains(colSchema.OriginalType, "pg:") && strings.Contains(colSchema.OriginalType, "int") {
			writeQuotedInt64(w, int64(v))
			return nil
		}
		if colSchema.DataType == ytschema.TypeFloat64.String() {
			// Will print all available float point numbers.
			writeQuotedFloat64(w, v, 'g', -1)
			return nil
		}
		writeQuotedFloat64(w, v, 'f', 6)
		return nil
	case []interface{}:
		var elemRep bytes.Buffer
		elemColSchema := BuildColSchemaArrayElement(colSchema)
		lBracket, rBracket := workaroundChooseBrackets(colSchema)
		_ = w.WriteByte('\'')
		_, _ = w.WriteString(lBracket)
		for i, value := range v {
			elemRep.Reset()
			err := recursiveRepresentToWriter(&elemRep, value, elemColSchema)
			if err != nil {
				return xerrors.Errorf("unable to represent array element: %w", err)
			}
			if i > 0 {
				_ = w.WriteByte(',')
			}
			elemBytes := elemRep.Bytes()
			if len(elemBytes) >= 2 && elemBytes[0] == '\'' && elemBytes[len(elemBytes)-1] == '\'' {
				elemBytes = elemBytes[1 : len(elemBytes)-1]
			}
			_, _ = w.Write(elemBytes)
		}
		_, _ = w.WriteString(rBracket)
		_ = w.WriteByte('\'')
		return nil
	default:
		_, _ = w.WriteString(fmt.Sprintf("'%v'", v))
		return nil
	}
}

func Represent(val interface{}, colSchema abstract.ColSchema) (string, error) {
	var sb bytes.Buffer
	if err := recursiveRepresentToWriter(&sb, val, colSchema); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func representWithCastToWriter(w *bytes.Buffer, v interface{}, colSchema abstract.ColSchema) error {
	if err := recursiveRepresentToWriter(w, v, colSchema); err != nil {
		return xerrors.Errorf("failed to represent %v in the form suitable for PostgreSQL sink: %w", v, err)
	}

	if strings.HasPrefix(colSchema.OriginalType, "pg:") && !IsUserDefinedType(&colSchema) {
		castTarget := strings.TrimPrefix(colSchema.OriginalType, "pg:")
		_, _ = w.WriteString("::")
		_, _ = w.WriteString(castTarget)
	}

	return nil
}

func workaroundChooseBrackets(colSchema abstract.ColSchema) (string, string) {
	if colSchema.OriginalType == "pg:json" || colSchema.OriginalType == "pg:jsonb" {
		return "[", "]"
	}
	return "{", "}"
}

const (
	PgDateFormat      = "2006-01-02"
	PgDatetimeFormat  = "2006-01-02 15:04:05Z07:00:00"
	PgTimestampFormat = "2006-01-02 15:04:05.999999Z07:00:00"
)

func RepresentWithCast(v interface{}, colSchema abstract.ColSchema) (string, error) {
	var sb bytes.Buffer
	if err := representWithCastToWriter(&sb, v, colSchema); err != nil {
		return "", err
	}

	return sb.String(), nil
}
