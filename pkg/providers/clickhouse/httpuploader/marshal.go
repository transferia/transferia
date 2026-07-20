package httpuploader

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"

	"github.com/goccy/go-json"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/logging/batching_logger"
	"github.com/transferia/transferia/pkg/providers/clickhouse/columntypes"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/util/castx"
	"github.com/valyala/fastjson/fastfloat"
	"go.ytsaurus.tech/library/go/core/log"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

const marshalErrTpl = "unable to serialize column %v (value type %T): %w"

type MarshallingRules struct {
	ColSchema      []abstract.ColSchema
	ColNameToIndex map[string]int
	ColTypes       columntypes.TypeMapping
	AnyAsString    bool

	gfMap    *GFMap
	optTypes []*columntypes.TypeDescription
}

func (r *MarshallingRules) SetColType(name string, description *columntypes.TypeDescription) {
	r.ColTypes[name] = description
	r.optTypes[r.ColNameToIndex[name]] = description
}

func NewRules(names []string, colSchema []abstract.ColSchema, colNameToIndex map[string]int, colTypes columntypes.TypeMapping, anyAsString bool) *MarshallingRules {
	optTypes := make([]*columntypes.TypeDescription, len(colNameToIndex))
	for k, v := range colNameToIndex {
		optTypes[v] = colTypes[k]
	}

	return &MarshallingRules{
		ColSchema:      colSchema,
		ColNameToIndex: colNameToIndex,
		ColTypes:       colTypes,
		AnyAsString:    anyAsString,

		optTypes: optTypes,
		gfMap:    NewGrishaFMap(names, colNameToIndex),
	}
}

func writeColName(buf *bytes.Buffer, colName string) int {
	buf.WriteByte('"')
	buf.WriteString(colName)
	buf.WriteByte('"')
	buf.WriteByte(':')
	return len(colName) + 3
}

func marshalTime(colType *columntypes.TypeDescription, v time.Time, buf *bytes.Buffer) {
	switch {
	case colType.IsString:
		_, _ = fmt.Fprintf(buf, "\"%s\"", v.Format("2006-01-02 15:04:05.999999999 -0700 MST"))
	case colType.IsDateTime64:
		fullTS := v.UnixNano()
		if colType.DateTime64Precision() > 0 && colType.DateTime64Precision() < 9 {
			fullTS = fullTS / int64(math.Pow(10, float64(9-colType.DateTime64Precision())))
		}
		_, _ = fmt.Fprintf(buf, "%d", fullTS)
	case colType.IsDate:
		_, _ = fmt.Fprintf(buf, "\"%v\"", v.Format("2006-01-02"))
	default:
		_, _ = fmt.Fprintf(buf, "%d", v.Unix())
	}
}

func MarshalCItoJSON(inLogger log.Logger, row abstract.ChangeItem, rules *MarshallingRules, buf *bytes.Buffer) error {
	buf.WriteByte('{')
	colNames := row.ColumnNames
	colValues := row.ColumnValues
	if row.Kind == abstract.DeleteKind {
		colNames = row.OldKeys.KeyNames
		colValues = row.OldKeys.KeyValues
	}
	hasColumns := false
	for idx, columnName := range colNames {
		// if colNames same as was used to build GFMap return result from slice, otherwise map lookup.
		colSchemaIndex, ok := rules.gfMap.Lookup(columnName, idx)
		if !ok {
			return abstract.NewFatalError(xerrors.Errorf("can't find colSchema for this columnName, columnName: %s", columnName))
		}
		colSchema := &rules.ColSchema[colSchemaIndex]
		if isNilValue(colValues[idx]) {
			continue
		}
		hLen := writeColName(buf, columnName)
		colType := rules.optTypes[colSchemaIndex]
		if colType == nil {
			return abstract.NewFatalError(xerrors.Errorf("unknown type for column '%s' in target table", columnName))
		}

		isSkip, err := marshalValue(inLogger, buf, colSchema, colType, rules.AnyAsString, columnName, colValues[idx], hLen)
		if err != nil {
			return err
		}
		if isSkip {
			// the column was skipped (the value marshaled to JSON null); skip it entirely
			continue
		}

		buf.WriteByte(',')
		hasColumns = true
	}
	if hasColumns {
		buf.Truncate(buf.Len() - 1)
	}
	buf.WriteByte('}')
	buf.WriteByte('\n')
	return nil
}

// TODO - remove this logger & throttler - after TM-10327
var logThrottlerMarshalValue batching_logger.Throttler

func init() {
	logThrottlerMarshalValue = batching_logger.NewSilentThrottler(batching_logger.NewConcurrentThrottler(batching_logger.NewIntervalThrottler(time.Hour)))
}

// marshalValue serializes a single column value into buf as a JSON value.
//
// The dispatch is driven by the schema data type (colSchema.DataType) — see the analogous
// switch in yt/sink/common.go restore(). Each case handles the Go type that
// changeitem/strictify.Strictify produces for that data type (e.g. bool for TypeBoolean,
// time.Time for the temporal types), and falls back to the generic JSON encoder otherwise.
// Decimals are the one cross-type corner case, handled before the dispatch.
//
// Every control-flow path writes a well-formed JSON value (or skips the column name and
// returns true), so the resulting row is always valid JSON.
func marshalValue(
	inLogger log.Logger,
	buf *bytes.Buffer,
	colSchema *abstract.ColSchema,
	colType *columntypes.TypeDescription,
	anyAsString bool,
	columnName string,
	val interface{},
	hLen int,
) (bool, error) {
	// typesystem version=1
	if (provider_postgres.IsPgTypeTimestampWithTimeZone(colSchema.OriginalType) || provider_postgres.IsPgTypeTimestampWithoutTimeZone(colSchema.OriginalType)) && colSchema.DataType == ytschema.TypeString.String() {
		switch v := val.(type) {
		case string: // pg->lb->ch
			buf.WriteString(v)
			return false, nil
		case time.Time: // pg->yt
			_, _ = fmt.Fprintf(buf, "\"%s\"", v.Format("2006-01-02 15:04:05.000000 -0700 MST"))
			return false, nil
		default:
			return false, xerrors.Errorf("unable type for pg:timestamp with time zone, type=%T, val=%v", val, val)
		}
	}
	if dateUnp, ok := val.(time.Time); ok && colSchema.OriginalType == "pg:date" && colSchema.DataType == ytschema.TypeString.String() {
		_, _ = fmt.Fprintf(buf, "\"%s\"", dateUnp.Format(time.RFC3339Nano))
		return false, nil
	}

	if colType.IsDecimal {
		// decimals arrive as ready-to-use numeric literals (any data type), emit them verbatim
		switch v := val.(type) {
		case string:
			buf.WriteString(v)
			return false, nil
		case []byte:
			buf.Write(v)
			return false, nil
		}
	}

	switch colSchema.DataType {
	case ytschema.TypeInt8.String(), ytschema.TypeInt16.String(), ytschema.TypeInt32.String(), ytschema.TypeInt64.String(),
		ytschema.TypeUint8.String(), ytschema.TypeUint16.String(), ytschema.TypeUint32.String(), ytschema.TypeUint64.String(),
		ytschema.TypeFloat32.String(), ytschema.TypeFloat64.String(),
		ytschema.TypeInterval.String():
		// numeric types render as JSON numbers (quoted when the target column is a string)
		handled, err := marshalNumericValue(buf, colType, val)
		if err != nil {
			return false, xerrors.Errorf("unable to marshal numeric value for column %q: %w", columnName, err)
		}
		if handled {
			return false, nil
		}
		// not a numeric Go value: fall back to the generic encoder below

		batching_logger.LogLine(logThrottlerMarshalValue, func(in string) { inLogger.Info(in) }, fmt.Sprintf("marshalValue - int fall into generic fallback, val_type: %T", val))

	case ytschema.TypeBytes.String(), ytschema.TypeString.String():
		// textual types render as quoted JSON strings, preserving raw (possibly non-utf8) bytes
		switch v := val.(type) {
		case string:
			writeQuoted(buf, v)
			return false, nil
		case []byte:
			writeBytesValue(buf, colType, v)
			return false, nil
		}

		batching_logger.LogLine(logThrottlerMarshalValue, func(in string) { inLogger.Info(in) }, fmt.Sprintf("marshalValue - bytes/string fall into generic fallback, val_type: %T", val))

	case ytschema.TypeBoolean.String():
		// strictify maps ytschema.TypeBoolean -> bool (cast.ToBoolE)
		if v, ok := val.(bool); ok {
			if v {
				buf.WriteString("true")
			} else {
				buf.WriteString("false")
			}
			return false, nil
		}

		batching_logger.LogLine(logThrottlerMarshalValue, func(in string) { inLogger.Info(in) }, fmt.Sprintf("marshalValue - boolean fall into generic fallback, val_type: %T", val))

	case ytschema.TypeDate.String(), ytschema.TypeDatetime.String(), ytschema.TypeTimestamp.String():
		// strictify maps ytschema.TypeDate / TypeDatetime / TypeTimestamp -> time.Time (cast.ToTimeE);
		// the actual JSON shape is driven by the target column type (colType), see marshalTime.
		switch v := val.(type) {
		case time.Time:
			marshalTime(colType, v, buf)
			return false, nil
		case *time.Time:
			marshalTime(colType, *v, buf)
			return false, nil
		}

		batching_logger.LogLine(logThrottlerMarshalValue, func(in string) { inLogger.Info(in) }, fmt.Sprintf("marshalValue - date fall into generic fallback, val_type: %T", val))

	default:
		// ytschema.TypeAny (strictify -> JSON-marshallable value) and any unknown data type fall
		// back to the generic JSON encoder below, which always produces a valid JSON value.

		batching_logger.LogLine(logThrottlerMarshalValue, func(in string) { inLogger.Info(in) }, fmt.Sprintf("marshalValue - 'switch colSchema.DataType' fall into default, colSchema.DataType: %s", colSchema.DataType))
	}

	batching_logger.LogLine(logThrottlerMarshalValue, func(in string) { inLogger.Info(in) }, fmt.Sprintf("marshalValue - marshalGeneric, colSchema.DataType: %s, valType:%T", colSchema.DataType, val))

	// Reached when the value's Go type did not match the column's data type above, or the type is
	// Any/unknown: the generic JSON encoder always produces a valid JSON value (or rolls back to null).
	isSkip, err := marshalGeneric(inLogger, buf, colSchema, colType, anyAsString, columnName, val, hLen)
	if err != nil {
		return isSkip, xerrors.Errorf("unable to marshal column %q (data type %s) with generic encoder: %w", columnName, colSchema.DataType, err)
	}
	return isSkip, nil
}

// marshalNumericValue writes numeric Go values as JSON numbers. It reports handled=false when val
// is not a numeric Go type so the caller can fall back to the generic encoder.
func marshalNumericValue(buf *bytes.Buffer, colType *columntypes.TypeDescription, val interface{}) (handled bool, err error) {
	var strV string
	switch v := val.(type) {
	case int, uint, int8, uint8, int16, uint16, int32, uint32, int64, uint64:
		s, err := castx.ToStringE(v)
		if err != nil {
			return true, xerrors.Errorf("unexpected cast: %w", err)
		}
		strV = s
	case float32:
		strV = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		strV = strconv.FormatFloat(v, 'f', -1, 64)
	case json.Number:
		strV = v.String()
		// Float64 conversion returns Inf and ErrRange for large values like 1E+3000.
		// It's not the real Inf so ignore it and keep the original literal, but ClickHouse
		// expects the bare nan/inf/-inf words for the actual non-finite values.
		if f, err := fastfloat.Parse(v.String()); err != nil {
			if xerrors.Is(err, strconv.ErrRange) {
				switch {
				case math.IsNaN(f):
					strV = "nan"
				case math.IsInf(f, 1):
					strV = "inf"
				case math.IsInf(f, -1):
					strV = "-inf"
				}
			} else {
				return true, xerrors.Errorf("error checking json.Number for nan/inf: %w", err)
			}
		}
	default:
		return false, nil
	}

	if colType.IsString {
		buf.WriteByte('"')
	}
	buf.WriteString(strV)
	if colType.IsString {
		buf.WriteByte('"')
	}
	return true, nil
}

// writeBytesValue writes a []byte value, either as a JSON array of byte values (for array columns)
// or as a quoted JSON string preserving the raw bytes.
func writeBytesValue(buf *bytes.Buffer, colType *columntypes.TypeDescription, v []byte) {
	if colType.IsArray {
		writeByteSliceAsArray(buf, v)
		return
	}
	// ClickHouse supports non-utf8 sequences in JSON strings at INSERT (an undocumented feature),
	// so we preserve the bytes as is and only escape what JSON strictly requires.
	writeQuoted(buf, v)
}

// TODO - remove this logger & throttler - after TM-10327
var logThrottlerMarshalGeneric batching_logger.Throttler

func init() {
	logThrottlerMarshalGeneric = batching_logger.NewSilentThrottler(batching_logger.NewConcurrentThrottler(batching_logger.NewIntervalThrottler(time.Hour)))
}

// marshalGeneric is the universal fallback encoder. It always produces a valid JSON value, or skips
// the already-written column name (returning true) when the value marshals to null.
func marshalGeneric(
	inLogger log.Logger,
	buf *bytes.Buffer,
	colSchema *abstract.ColSchema,
	colType *columntypes.TypeDescription,
	anyAsString bool,
	columnName string,
	val interface{},
	hLen int,
) (bool, error) {
	switch v := val.(type) {
	case string:
		writeQuoted(buf, v)
		return false, nil
	case []byte:
		writeBytesValue(buf, colType, v)
		return false, nil
	default:
		r, err := json.Marshal(v)
		if err != nil {
			return false, xerrors.Errorf(marshalErrTpl, columnName, v, err)
		}
		if bytes.Equal(r, []byte("null")) {
			// skip column: value is null
			buf.Truncate(buf.Len() - hLen)
			return true, nil
		}
		if colSchema.DataType != ytschema.TypeAny.String() || anyAsString || colType.IsString {
			batching_logger.LogLine(logThrottlerMarshalGeneric, func(in string) { inLogger.Info(in) }, fmt.Sprintf("marshalGeneric - DOUBLE_MARSHAL, colSchema.DataType: %s, valType:%T, anyAsString:%t, colType.IsString:%t", colSchema.DataType, val, anyAsString, colType.IsString))
			rr, err := json.Marshal(string(r))
			if err != nil {
				return false, xerrors.Errorf(marshalErrTpl, columnName, v, err)
			}
			buf.Write(rr)
		} else {
			buf.Write(r)
		}
		return false, nil
	}
}

func isNilValue(v interface{}) bool {
	if v == nil {
		return true
	}
	vv := reflect.ValueOf(v)
	return vv.Kind() == reflect.Pointer && vv.IsNil()
}

const hexChars = "0123456789abcdef"

// writeQuoted writes s as a quoted JSON string. Bytes >= 0x20 (including non-utf8 bytes) are
// preserved as is; only the characters JSON strictly requires (", \ and control bytes) are escaped.
//
// It works for both string and []byte. For the []byte case the string(...) conversions below do not
// allocate: bytes.Buffer.WriteString does not retain its argument, so the compiler lowers them to a
// non-escaping temporary view of the slice.
func writeQuoted[T ~string | ~[]byte](buf *bytes.Buffer, s T) {
	buf.WriteByte('"')
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 0x20 && c != '"' && c != '\\' {
			continue
		}
		if start < i {
			buf.WriteString(string(s[start:i]))
		}
		writeEscapedByte(buf, c)
		start = i + 1
	}
	if start < len(s) {
		buf.WriteString(string(s[start:]))
	}
	buf.WriteByte('"')
}

// writeEscapedByte writes the JSON escape sequence for c, which must be ", \ or a control byte (< 0x20).
func writeEscapedByte(buf *bytes.Buffer, c byte) {
	switch c {
	case '"':
		buf.WriteString(`\"`)
	case '\\':
		buf.WriteString(`\\`)
	case '\n':
		buf.WriteString(`\n`)
	case '\r':
		buf.WriteString(`\r`)
	case '\t':
		buf.WriteString(`\t`)
	case '\f':
		buf.WriteString(`\f`)
	case '\b':
		buf.WriteString(`\b`)
	default:
		buf.WriteString(`\u00`)
		buf.WriteByte(hexChars[c>>4])
		buf.WriteByte(hexChars[c&0xF])
	}
}

// writeByteSliceAsArray writes b as a JSON array of its byte values, e.g. [1,2,3].
func writeByteSliceAsArray(buf *bytes.Buffer, b []byte) {
	buf.WriteByte('[')
	for i, x := range b {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(strconv.Itoa(int(x)))
	}
	buf.WriteByte(']')
}
