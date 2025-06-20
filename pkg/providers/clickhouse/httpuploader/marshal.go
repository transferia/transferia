//go:build !disable_clickhouse_provider

package httpuploader

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/clickhouse/columntypes"
	"github.com/transferia/transferia/pkg/util/castx"
	"github.com/valyala/fastjson/fastfloat"
	"go.ytsaurus.tech/yt/go/schema"
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
		fmt.Fprintf(buf, "\"%s\"", v.Format("2006-01-02 15:04:05.999999999 -0700 MST"))
	case colType.IsDateTime64:
		fullTS := v.UnixNano()
		if colType.DateTime64Precision() > 0 && colType.DateTime64Precision() < 9 {
			fullTS = fullTS / int64(math.Pow(10, float64(9-colType.DateTime64Precision())))
		}
		fmt.Fprintf(buf, "%d", fullTS)
	case colType.IsDate:
		fmt.Fprintf(buf, "\"%v\"", v.Format("2006-01-02"))
	default:
		fmt.Fprintf(buf, "%d", v.Unix())
	}
}

func marshalByteSliceAsArray(slice []byte) string {
	result := make([]string, len(slice))
	for i, b := range slice {
		result[i] = strconv.Itoa(int(b))
	}
	return fmt.Sprintf("[%s]", strings.Join(result, ","))
}

func MarshalCItoJSON(row abstract.ChangeItem, rules *MarshallingRules, buf *bytes.Buffer) error {
	buf.WriteByte('{')
	colNames := row.ColumnNames
	colValues := row.ColumnValues
	if row.Kind == abstract.DeleteKind {
		colNames = row.OldKeys.KeyNames
		colValues = row.OldKeys.KeyValues
	}
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
			return abstract.NewFatalError(xerrors.Errorf("unknown type for column %s in target table", columnName))
		}

		switch v := colValues[idx].(type) {
		case time.Time:
			marshalTime(colType, v, buf)
		case *time.Time:
			marshalTime(colType, *v, buf)
		case string:
			if columntypes.LegacyIsDecimal(colSchema.OriginalType) || colType.IsDecimal {
				buf.WriteString(v)
				break
			}
			buf.WriteString(`"`)
			if bytes.ContainsRune([]byte(v), rune('"')) || bytes.ContainsRune([]byte(v), rune('\\')) {
				// We  have a " symbol in value, so we must quote string before submit it
				// we want to preserve bytes as is since, clickhouse can accept them and store properly
				// that's why we use this custom quote func
				buf.WriteString(questionableQuoter(v))
			} else {
				buf.WriteString(v)
			}
			buf.WriteString(`"`)
		case int, uint, int8, uint8, int16, uint16, int32, uint32, int64, uint64,
			*int, *uint, *int8, *uint8, *int16, *uint16, *int32, *uint32, *int64, *uint64:
			if colType.IsString {
				buf.WriteByte('"')
			}
			strV, err := castx.ToStringE(v)
			if err != nil {
				return xerrors.Errorf("unexpected cast: %w", err)
			}
			buf.WriteString(strV)
			if colType.IsString {
				buf.WriteByte('"')
			}
		case float32, float64, json.Number:
			var strV string
			switch vv := v.(type) {
			case float32:
				strV = strconv.FormatFloat(float64(vv), 'f', -1, 32)
			case float64:
				strV = strconv.FormatFloat(vv, 'f', -1, 64)
			case json.Number:
				// Float64 conversion returns Inf and ErrRange for large values like 1E+3000
				// It's not the real Inf so ignore it
				if v, err := fastfloat.Parse(vv.String()); err != nil {
					if xerrors.Is(err, strconv.ErrRange) {
						if math.IsNaN(v) {
							strV = "nan"
							break
						}
						if math.IsInf(v, 1) {
							strV = "inf"
							break
						}
						if math.IsInf(v, -1) {
							strV = "-inf"
							break
						}
					} else {
						return xerrors.Errorf("error checking json.Number for nan/inf: %w", err)
					}
				}
				strV = vv.String()
			default:
				return xerrors.Errorf("unexpected float type: %T (value=`%v`)", v, v)
			}

			if colType.IsString {
				buf.WriteByte('"')
			}
			buf.WriteString(strV)
			if colType.IsString {
				buf.WriteByte('"')
			}
		case bool:
			if colSchema.DataType == schema.TypeBoolean.String() {
				if v {
					buf.WriteString("true")
				} else {
					buf.WriteString("false")
				}
			} else if (strings.ToLower(colSchema.DataType) == "any" && rules.AnyAsString) || colType.IsString {
				if v {
					buf.WriteString(`"true"`)
				} else {
					buf.WriteString(`"false"`)
				}
			} else {
				if v {
					buf.WriteString(`1`)
				} else {
					buf.WriteString(`0`)
				}
			}
		case []byte:
			if columntypes.LegacyIsDecimal(colSchema.OriginalType) || colType.IsDecimal {
				buf.WriteString(string(v))
				break
			}
			if colType.IsArray {
				buf.WriteString(marshalByteSliceAsArray(v))
				break
			}
			if bytes.ContainsRune(v, rune('"')) || bytes.ContainsRune(v, rune('\\')) {
				// We  have a " symbol in value, so we must quote string before submit it
				// we want to preserve bytes as is since, clickhouse can accept them and store properly
				// that's why we use this custom quote func
				buf.WriteString(fmt.Sprintf(`"%s"`, questionableQuoter(string(v))))
				break
			}
			// ClickHouse supports non-UTF-8 sequences in JSON at INSERT. This is (currently?) and undocumented feature
			buf.WriteString(fmt.Sprintf(`"%s"`, string(v)))
		default:
			r, err := json.Marshal(v)
			if err != nil {
				return xerrors.Errorf(marshalErrTpl, columnName, v, err)
			}
			if string(r) == "null" {
				// rollback column name
				buf.Truncate(buf.Len() - hLen)
				continue
			}
			if strings.ToLower(colSchema.DataType) != "any" || rules.AnyAsString || colType.IsString {
				rr, err := json.Marshal(string(r))
				if err != nil {
					return xerrors.Errorf(marshalErrTpl, columnName, v, err)
				}
				buf.Write(rr)
			} else {
				buf.Write(r)
			}
		}
		buf.WriteByte(',')
	}
	buf.Truncate(buf.Len() - 1)
	buf.WriteByte('}')
	buf.WriteByte('\n')
	return nil
}

func isNilValue(v interface{}) bool {
	if v == nil {
		return true
	}
	vv := reflect.ValueOf(v)
	return vv.Kind() == reflect.Pointer && vv.IsNil()
}

// questionableQuoter is like strconv.Quote, but do not try to escape non-utf8 chars
func questionableQuoter(v string) string {
	return strings.ReplaceAll(strings.ReplaceAll(v, `\`, `\\`), `"`, `\"`)
}
