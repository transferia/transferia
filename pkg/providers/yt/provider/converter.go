package provider

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	yt_table "github.com/transferia/transferia/pkg/providers/yt/provider/table"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/skiff"
)

// rowConverter converts a Skiff-decoded struct row and the row's index into a flat []interface{}
// suitable for direct assignment to ChangeItem.ColumnValues.
// rowIDX is the physical row offset used to populate synthetic index columns.
type rowConverter func(row reflect.Value, rowIDX int64) ([]interface{}, error)

func skiffGoType(col yt_table.YtColumn) reflect.Type {
	ytType, ok := col.YtType().(ytschema.Type)
	if !ok {
		// Complex type (List, Dict, Tuple, etc.) — always interface{}.
		return reflect.TypeFor[any]()
	}

	var base reflect.Type
	switch ytType {
	case ytschema.TypeInt8:
		base = reflect.TypeFor[int8]()
	case ytschema.TypeInt16:
		base = reflect.TypeFor[int16]()
	case ytschema.TypeInt32:
		base = reflect.TypeFor[int32]()
	case ytschema.TypeInt64, ytschema.TypeInterval:
		base = reflect.TypeFor[int64]()
	case ytschema.TypeUint8:
		base = reflect.TypeFor[uint8]()
	case ytschema.TypeUint16:
		base = reflect.TypeFor[uint16]()
	case ytschema.TypeUint32:
		base = reflect.TypeFor[uint32]()
	case ytschema.TypeUint64, ytschema.TypeDate, ytschema.TypeDatetime, ytschema.TypeTimestamp:
		base = reflect.TypeFor[uint64]()
	case ytschema.TypeFloat32, ytschema.TypeFloat64:
		// Both YT float types map to Skiff TypeDouble (float64).
		base = reflect.TypeFor[float64]()
	case ytschema.TypeBoolean:
		base = reflect.TypeFor[bool]()
	case ytschema.TypeString, ytschema.TypeBytes:
		// Both map to Skiff TypeString32; decoder writes a string value.
		base = reflect.TypeFor[string]()
	default:
		// TypeAny → TypeYSON32; decoder calls yson.Unmarshal and writes interface{}.
		base = reflect.TypeFor[any]()
	}
	if col.Nullable() {
		return reflect.PointerTo(base)
	}
	return base
}

// ytSchemaForSkiff builds a ytschema.Schema for use with skiff.FromTableSchema,
// skipping the synthetic row-index column (skipColName) that has no wire representation.
func ytSchemaForSkiff(tbl yt_table.YtTable, skipColName string) ytschema.Schema {
	var cols []ytschema.Column
	for i := 0; i < tbl.ColumnsCount(); i++ {
		col := tbl.Column(i).(yt_table.YtColumn)
		if skipColName != "" && col.Name() == skipColName {
			continue
		}
		ct := col.YtType()
		if col.Nullable() {
			ct = ytschema.Optional{Item: col.YtType()}
		}
		cols = append(cols, ytschema.Column{
			Name:        col.Name(),
			ComplexType: ct,
			Required:    !col.Nullable(),
		})
	}
	strict := true
	return ytschema.Schema{Columns: cols, Strict: &strict}
}

// buildSkiffFormat returns a pointer to the skiff.Format to pass to yt.ReadTableOptions.Format
// and to store on snapshotSource.
func buildSkiffFormat(tbl yt_table.YtTable, idxColName string) *skiff.Format {
	ytSchema := ytSchemaForSkiff(tbl, idxColName)
	skiffSchema := skiff.FromTableSchema(ytSchema)
	f := skiff.Format{Name: "skiff", TableSchemas: []any{&skiffSchema}}
	return &f
}

// buildSkiffRowType builds a reflect.StructOf type that the Skiff decoder will populate.
// Fields are named F0..FN-1 with yson:"colname" tags; the synthetic idx column is excluded.
func buildSkiffRowType(tbl yt_table.YtTable, idxColName string) reflect.Type {
	var fields []reflect.StructField
	for i := 0; i < tbl.ColumnsCount(); i++ {
		col := tbl.Column(i).(yt_table.YtColumn)
		if idxColName != "" && col.Name() == idxColName {
			continue
		}
		fields = append(fields, reflect.StructField{
			Name: fmt.Sprintf("F%d", len(fields)),
			Type: skiffGoType(col),
			Tag:  reflect.StructTag(`yson:"` + col.Name() + `"`),
		})
	}
	return reflect.StructOf(fields)
}

// nativeCastField converts a decoded struct field value to a native Go type
// that can be placed directly in ChangeItem.ColumnValues.
// Returns the same types that abstract2.Value.ToOldValue() would return for primitive types.
func nativeCastField(fv reflect.Value, col yt_table.YtColumn) (interface{}, error) {
	if col.Nullable() {
		if fv.IsNil() {
			return nil, nil
		}
		fv = fv.Elem()
	}

	ytType, ok := col.YtType().(ytschema.Type)
	if !ok {
		return fv.Interface(), nil
	}

	switch ytType {
	case ytschema.TypeInt8:
		return int8(fv.Int()), nil
	case ytschema.TypeInt16:
		return int16(fv.Int()), nil
	case ytschema.TypeInt32:
		return int32(fv.Int()), nil
	case ytschema.TypeInt64:
		return fv.Int(), nil
	case ytschema.TypeUint8:
		return uint8(fv.Uint()), nil
	case ytschema.TypeUint16:
		return uint16(fv.Uint()), nil
	case ytschema.TypeUint32:
		return uint32(fv.Uint()), nil
	case ytschema.TypeUint64:
		return fv.Uint(), nil
	case ytschema.TypeFloat32:
		return float32(fv.Float()), nil
	case ytschema.TypeFloat64:
		return fv.Float(), nil
	case ytschema.TypeBoolean:
		return fv.Bool(), nil
	case ytschema.TypeString:
		return fv.String(), nil
	case ytschema.TypeBytes:
		return []byte(fv.String()), nil
	case ytschema.TypeDate:
		return time.Unix(int64(fv.Uint())*86400, 0).UTC(), nil
	case ytschema.TypeDatetime:
		return time.Unix(int64(fv.Uint()), 0).UTC(), nil
	case ytschema.TypeTimestamp:
		return time.UnixMicro(int64(fv.Uint())).UTC(), nil
	case ytschema.TypeInterval:
		return time.Duration(fv.Int()) * time.Microsecond, nil
	default:
		// TypeAny / TypeNull: field is interface{}, decoder already called yson.Unmarshal.
		return fv.Interface(), nil
	}
}

// makeRowConverter builds a rowConverter closure pre-computed for tbl.
// Call once at table open; the returned function is the per-row hot path.
// idxColName is the synthetic row-index column added by AddRowIdxColumn (empty if unused).
func makeRowConverter(tbl yt_table.YtTable, idxColName string) rowConverter {
	cnt := tbl.ColumnsCount()

	type colMeta struct {
		col       yt_table.YtColumn
		isSynth   bool
		structIdx int // position in the Skiff struct (excludes the synthetic column)
	}
	metas := make([]colMeta, cnt)
	structIdx := 0
	for i := 0; i < cnt; i++ {
		col := tbl.Column(i).(yt_table.YtColumn)
		if idxColName != "" && col.Name() == idxColName {
			metas[i] = colMeta{col: col, isSynth: true, structIdx: 0}
		} else {
			metas[i] = colMeta{col: col, isSynth: false, structIdx: structIdx}
			structIdx++
		}
	}

	return func(row reflect.Value, rowIDX int64) ([]interface{}, error) {
		values := make([]interface{}, cnt)
		for i, m := range metas {
			if m.isSynth {
				// AddRowIdxColumn always adds TypeInt64.
				values[i] = rowIDX
				continue
			}
			v, err := nativeCastField(row.Field(m.structIdx), m.col)
			if err != nil {
				return nil, xerrors.Errorf("column %s: %w", m.col.Name(), err)
			}
			values[i] = v
		}
		return values, nil
	}
}

func tableHasComplexColumns(tbl yt_table.YtTable, idxColName string) bool {
	for i := 0; i < tbl.ColumnsCount(); i++ {
		col := tbl.Column(i).(yt_table.YtColumn)
		if idxColName != "" && col.Name() == idxColName {
			continue
		}
		if _, isPrimitive := col.YtType().(ytschema.Type); !isPrimitive {
			return true
		}
	}
	return false
}

// wireSizeForPrimitive returns the Skiff wire-format fixed size for a YT primitive type.
func wireSizeForPrimitive(t ytschema.Type) int {
	switch t {
	case ytschema.TypeBoolean, ytschema.TypeInt8, ytschema.TypeUint8:
		return 1
	case ytschema.TypeInt16, ytschema.TypeUint16:
		return 2
	case ytschema.TypeInt32, ytschema.TypeUint32:
		return 4
	case ytschema.TypeInt64, ytschema.TypeUint64,
		ytschema.TypeFloat32, ytschema.TypeFloat64,
		ytschema.TypeInterval,
		ytschema.TypeDate, ytschema.TypeDatetime, ytschema.TypeTimestamp:
		return 8
	default:
		return 0
	}
}

// rowSizeEstimator pre-computes Skiff wire-size baseline from the table schema
// at table-open time. The hot-path Estimate() only adds variable-length data.
type rowSizeEstimator struct {
	baseline      int
	varColIdx     []int // indices in values[] for primitive string/bytes columns
	complexColIdx []int // indices in values[] for complex + TypeAny columns
}

func makeRowSizeEstimator(tbl yt_table.YtTable, idxColName string) *rowSizeEstimator {
	var (
		baseline      int
		varColIdx     []int
		complexColIdx []int
	)

	for i := 0; i < tbl.ColumnsCount(); i++ {
		col := tbl.Column(i).(yt_table.YtColumn)
		if idxColName != "" && col.Name() == idxColName {
			baseline += 8 // synthetic row-index: int64 on wire
			continue
		}
		colBase := 0
		if col.Nullable() {
			colBase++ // Variant8 tag byte
		}

		ytType, isPrimitive := col.YtType().(ytschema.Type)
		if !isPrimitive {
			// Complex type → YSON32: 4-byte length prefix + variable data.
			// Data content measured at estimate time via walkValueSize.
			colBase += 4
			baseline += colBase
			complexColIdx = append(complexColIdx, i)
			continue
		}

		switch ytType {
		case ytschema.TypeString, ytschema.TypeBytes:
			colBase += 4 // String32 length prefix; data measured on hot path
			baseline += colBase
			varColIdx = append(varColIdx, i)
		case ytschema.TypeAny:
			colBase += 4 // YSON32 prefix; data measured at estimate time via walkValueSize
			complexColIdx = append(complexColIdx, i)
			baseline += colBase
		default:
			colBase += wireSizeForPrimitive(ytType)
			baseline += colBase
		}
	}

	return &rowSizeEstimator{baseline: baseline, varColIdx: varColIdx, complexColIdx: complexColIdx}
}

func (e *rowSizeEstimator) estimate(values []interface{}) int {
	n := e.baseline
	for _, idx := range e.varColIdx {
		switch v := values[idx].(type) {
		case string:
			n += len(v)
		case []byte:
			n += len(v)
		}
	}
	for _, idx := range e.complexColIdx {
		if idx < len(values) {
			n += provider_yt.WalkValueSize(values[idx], provider_yt.MaxComplexWalkDepth)
		}
	}
	return n
}

// mapRowToValues converts a YSON-decoded map row into a flat []interface{}
// suitable for direct assignment to ChangeItem.ColumnValues.
func mapRowToValues(row map[string]any, rowIdx int64, cols []yt_table.YtColumn, idxColName string) ([]interface{}, error) {
	values := make([]interface{}, len(cols))
	for i, col := range cols {
		if idxColName != "" && col.Name() == idxColName {
			values[i] = rowIdx
			continue
		}
		v, err := castMapValue(row[col.Name()], col)
		if err != nil {
			return nil, xerrors.Errorf("column %s: %w", col.Name(), err)
		}
		values[i] = v
	}
	return values, nil
}

// castMapValue narrows values produced by the YSON map-decode path to the Go
// types expected for each YT type. The YSON decoder always materializes
// integers as int64/uint64 and floats as float64 regardless of column width,
// so we only need a single type assertion before narrowing.
func castMapValue(v any, col yt_table.YtColumn) (interface{}, error) {
	if v == nil {
		return nil, nil
	}

	ytType, ok := col.YtType().(ytschema.Type)
	if !ok {
		return v, nil
	}

	switch ytType {
	case ytschema.TypeInt8:
		i, ok := v.(int64)
		if !ok {
			return nil, xerrors.Errorf("int8: expected int64, got %T", v)
		}
		return int8(i), nil
	case ytschema.TypeInt16:
		i, ok := v.(int64)
		if !ok {
			return nil, xerrors.Errorf("int16: expected int64, got %T", v)
		}
		return int16(i), nil
	case ytschema.TypeInt32:
		i, ok := v.(int64)
		if !ok {
			return nil, xerrors.Errorf("int32: expected int64, got %T", v)
		}
		return int32(i), nil
	case ytschema.TypeInt64:
		i, ok := v.(int64)
		if !ok {
			return nil, xerrors.Errorf("int64: expected int64, got %T", v)
		}
		return i, nil
	case ytschema.TypeUint8:
		u, ok := v.(uint64)
		if !ok {
			return nil, xerrors.Errorf("uint8: expected uint64, got %T", v)
		}
		return uint8(u), nil
	case ytschema.TypeUint16:
		u, ok := v.(uint64)
		if !ok {
			return nil, xerrors.Errorf("uint16: expected uint64, got %T", v)
		}
		return uint16(u), nil
	case ytschema.TypeUint32:
		u, ok := v.(uint64)
		if !ok {
			return nil, xerrors.Errorf("uint32: expected uint64, got %T", v)
		}
		return uint32(u), nil
	case ytschema.TypeUint64:
		u, ok := v.(uint64)
		if !ok {
			return nil, xerrors.Errorf("uint64: expected uint64, got %T", v)
		}
		return u, nil
	case ytschema.TypeFloat32:
		f, ok := v.(float64)
		if !ok {
			return nil, xerrors.Errorf("float32: expected float64, got %T", v)
		}
		return float32(f), nil
	case ytschema.TypeFloat64:
		f, ok := v.(float64)
		if !ok {
			return nil, xerrors.Errorf("float64: expected float64, got %T", v)
		}
		return json.Number(strconv.FormatFloat(f, 'f', -1, 64)), nil
	case ytschema.TypeBoolean:
		b, ok := v.(bool)
		if !ok {
			return nil, xerrors.Errorf("expected bool, got %T", v)
		}
		return b, nil
	case ytschema.TypeString:
		s, ok := v.(string)
		if !ok {
			return nil, xerrors.Errorf("expected string, got %T", v)
		}
		return s, nil
	case ytschema.TypeBytes:
		s, ok := v.(string)
		if !ok {
			return nil, xerrors.Errorf("expected string, got %T", v)
		}
		return []byte(s), nil
	case ytschema.TypeDate:
		u, ok := v.(uint64)
		if !ok {
			return nil, xerrors.Errorf("date: expected uint64, got %T", v)
		}
		return time.Unix(int64(u)*86400, 0).UTC(), nil
	case ytschema.TypeDatetime:
		u, ok := v.(uint64)
		if !ok {
			return nil, xerrors.Errorf("datetime: expected uint64, got %T", v)
		}
		return time.Unix(int64(u), 0).UTC(), nil
	case ytschema.TypeTimestamp:
		u, ok := v.(uint64)
		if !ok {
			return nil, xerrors.Errorf("timestamp: expected uint64, got %T", v)
		}
		return time.UnixMicro(int64(u)).UTC(), nil
	case ytschema.TypeInterval:
		i, ok := v.(int64)
		if !ok {
			return nil, xerrors.Errorf("interval: expected int64, got %T", v)
		}
		return time.Duration(i) * time.Microsecond, nil
	default:
		return v, nil
	}
}
