package arena

import (
	"reflect"
	"time"

	yt_table "github.com/transferia/transferia/pkg/providers/yt/provider/table"
	"github.com/transferia/transferia/pkg/util"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

const defaultChunk = 1024

// cellConverter reads one column from the decoded reflect struct, boxes the
// value into the corresponding arena, and returns a forged interface{} that
// points at the arena slot. Nullable handling and the column's structIdx are
// captured at build time in NewConverter, so the hot loop stays branch-free.
type cellConverter func(ac *Converter, row reflect.Value, rowIDX int64) any

// Converter is the arena-boxing replacement for per-cell interface{} boxing on
// the primitive-schema Skiff decode path. It keeps one per-Go-type chunked
// arena and, on each row, invokes a pre-built cellConverter per column that
// writes the cell's value into the appropriate arena and forges an interface{}
// pointing into the slot. Downstream sees the same concrete Go types as before
// (e.g., int8, string, time.Time), so ChangeItem contracts are untouched.
//
// Not safe for concurrent use. rowDecoder.cloneForReader clones per reader.
type Converter struct {
	converters []cellConverter
	chunkCap   int

	ai8    *arena[int8]
	ai16   *arena[int16]
	ai32   *arena[int32]
	ai64   *arena[int64]
	au8    *arena[uint8]
	au16   *arena[uint16]
	au32   *arena[uint32]
	au64   *arena[uint64]
	af32   *arena[float32]
	af64   *arena[float64]
	ab     *arena[bool]
	astr   *arena[string]
	abytes *arena[[]byte]
	atm    *arena[time.Time]
	adur   *arena[time.Duration]
}

// NewConverter builds a Converter for the given table schema. idxColName is an
// optional synthetic row-index column name (empty string when not needed).
func NewConverter(tbl yt_table.YtTable, idxColName string) *Converter {
	ac := freshConverter(defaultChunk)
	cnt := tbl.ColumnsCount()
	ac.converters = make([]cellConverter, cnt)
	structIdx := 0
	for i := 0; i < cnt; i++ {
		c := tbl.Column(i).(yt_table.YtColumn)
		if idxColName != "" && c.Name() == idxColName {
			ac.converters[i] = synthConverter
			continue
		}
		ac.converters[i] = makeCellConverter(c, structIdx)
		structIdx++
	}
	return ac
}

func freshConverter(chunkCap int) *Converter {
	return &Converter{
		converters: nil, // populated by NewConverter after schema is known
		chunkCap:   chunkCap,
		ai8:        newArena[int8](chunkCap),
		ai16:       newArena[int16](chunkCap),
		ai32:       newArena[int32](chunkCap),
		ai64:       newArena[int64](chunkCap),
		au8:        newArena[uint8](chunkCap),
		au16:       newArena[uint16](chunkCap),
		au32:       newArena[uint32](chunkCap),
		au64:       newArena[uint64](chunkCap),
		af32:       newArena[float32](chunkCap),
		af64:       newArena[float64](chunkCap),
		ab:         newArena[bool](chunkCap),
		astr:       newArena[string](chunkCap),
		abytes:     newArena[[]byte](chunkCap),
		atm:        newArena[time.Time](chunkCap),
		adur:       newArena[time.Duration](chunkCap),
	}
}

// synthConverter boxes the synthetic row-index int64 without reading from the
// row. It's schema-independent, so a single package-level value is reused.
var synthConverter cellConverter = func(ac *Converter, _ reflect.Value, rowIDX int64) any {
	return makeIface(typInt64, ac.ai64.put(rowIDX))
}

// makeCellConverter returns a closure specialized to (ytType, isNullable,
// structIdx) so the row hot loop is a plain range with no per-cell branching.
// Types that don't map to a primitive arena (TypeAny, composites, and anything
// yson-decoded as interface{}) fall through to an any-copy path with no arena.
func makeCellConverter(c yt_table.YtColumn, structIdx int) cellConverter {
	isNullable := c.Nullable()
	ytType, ok := c.YtType().(ytschema.Type)
	if !ok {
		return anyConverter(structIdx, isNullable)
	}
	// Two returns per case: the non-nullable branch skips the per-cell IsNil
	// check that the nullable branch must run. Folding them into one closure
	// that reads isNullable at runtime adds a branch on every cell and costs
	// roughly two extra CPU-minutes per billion rows on wide tables.
	switch ytType {
	case ytschema.TypeInt8:
		if isNullable {
			return func(ac *Converter, row reflect.Value, _ int64) any {
				fv := row.Field(structIdx)
				if fv.IsNil() {
					return nil
				}
				return makeIface(typInt8, ac.ai8.put(int8(fv.Elem().Int())))
			}
		}
		return func(ac *Converter, row reflect.Value, _ int64) any {
			return makeIface(typInt8, ac.ai8.put(int8(row.Field(structIdx).Int())))
		}
	case ytschema.TypeInt16:
		if isNullable {
			return func(ac *Converter, row reflect.Value, _ int64) any {
				fv := row.Field(structIdx)
				if fv.IsNil() {
					return nil
				}
				return makeIface(typInt16, ac.ai16.put(int16(fv.Elem().Int())))
			}
		}
		return func(ac *Converter, row reflect.Value, _ int64) any {
			return makeIface(typInt16, ac.ai16.put(int16(row.Field(structIdx).Int())))
		}
	case ytschema.TypeInt32:
		if isNullable {
			return func(ac *Converter, row reflect.Value, _ int64) any {
				fv := row.Field(structIdx)
				if fv.IsNil() {
					return nil
				}
				return makeIface(typInt32, ac.ai32.put(int32(fv.Elem().Int())))
			}
		}
		return func(ac *Converter, row reflect.Value, _ int64) any {
			return makeIface(typInt32, ac.ai32.put(int32(row.Field(structIdx).Int())))
		}
	case ytschema.TypeInt64:
		if isNullable {
			return func(ac *Converter, row reflect.Value, _ int64) any {
				fv := row.Field(structIdx)
				if fv.IsNil() {
					return nil
				}
				return makeIface(typInt64, ac.ai64.put(fv.Elem().Int()))
			}
		}
		return func(ac *Converter, row reflect.Value, _ int64) any {
			return makeIface(typInt64, ac.ai64.put(row.Field(structIdx).Int()))
		}
	case ytschema.TypeUint8:
		if isNullable {
			return func(ac *Converter, row reflect.Value, _ int64) any {
				fv := row.Field(structIdx)
				if fv.IsNil() {
					return nil
				}
				return makeIface(typUint8, ac.au8.put(uint8(fv.Elem().Uint())))
			}
		}
		return func(ac *Converter, row reflect.Value, _ int64) any {
			return makeIface(typUint8, ac.au8.put(uint8(row.Field(structIdx).Uint())))
		}
	case ytschema.TypeUint16:
		if isNullable {
			return func(ac *Converter, row reflect.Value, _ int64) any {
				fv := row.Field(structIdx)
				if fv.IsNil() {
					return nil
				}
				return makeIface(typUint16, ac.au16.put(uint16(fv.Elem().Uint())))
			}
		}
		return func(ac *Converter, row reflect.Value, _ int64) any {
			return makeIface(typUint16, ac.au16.put(uint16(row.Field(structIdx).Uint())))
		}
	case ytschema.TypeUint32:
		if isNullable {
			return func(ac *Converter, row reflect.Value, _ int64) any {
				fv := row.Field(structIdx)
				if fv.IsNil() {
					return nil
				}
				return makeIface(typUint32, ac.au32.put(uint32(fv.Elem().Uint())))
			}
		}
		return func(ac *Converter, row reflect.Value, _ int64) any {
			return makeIface(typUint32, ac.au32.put(uint32(row.Field(structIdx).Uint())))
		}
	case ytschema.TypeUint64:
		if isNullable {
			return func(ac *Converter, row reflect.Value, _ int64) any {
				fv := row.Field(structIdx)
				if fv.IsNil() {
					return nil
				}
				return makeIface(typUint64, ac.au64.put(fv.Elem().Uint()))
			}
		}
		return func(ac *Converter, row reflect.Value, _ int64) any {
			return makeIface(typUint64, ac.au64.put(row.Field(structIdx).Uint()))
		}
	case ytschema.TypeFloat32:
		if isNullable {
			return func(ac *Converter, row reflect.Value, _ int64) any {
				fv := row.Field(structIdx)
				if fv.IsNil() {
					return nil
				}
				return makeIface(typFloat32, ac.af32.put(float32(fv.Elem().Float())))
			}
		}
		return func(ac *Converter, row reflect.Value, _ int64) any {
			return makeIface(typFloat32, ac.af32.put(float32(row.Field(structIdx).Float())))
		}
	case ytschema.TypeFloat64:
		if isNullable {
			return func(ac *Converter, row reflect.Value, _ int64) any {
				fv := row.Field(structIdx)
				if fv.IsNil() {
					return nil
				}
				return makeIface(typFloat64, ac.af64.put(fv.Elem().Float()))
			}
		}
		return func(ac *Converter, row reflect.Value, _ int64) any {
			return makeIface(typFloat64, ac.af64.put(row.Field(structIdx).Float()))
		}
	case ytschema.TypeBoolean:
		if isNullable {
			return func(ac *Converter, row reflect.Value, _ int64) any {
				fv := row.Field(structIdx)
				if fv.IsNil() {
					return nil
				}
				return makeIface(typBool, ac.ab.put(fv.Elem().Bool()))
			}
		}
		return func(ac *Converter, row reflect.Value, _ int64) any {
			return makeIface(typBool, ac.ab.put(row.Field(structIdx).Bool()))
		}
	case ytschema.TypeString:
		if isNullable {
			return func(ac *Converter, row reflect.Value, _ int64) any {
				fv := row.Field(structIdx)
				if fv.IsNil() {
					return nil
				}
				return makeIface(typString, ac.astr.put(fv.Elem().String()))
			}
		}
		return func(ac *Converter, row reflect.Value, _ int64) any {
			return makeIface(typString, ac.astr.put(row.Field(structIdx).String()))
		}
	case ytschema.TypeBytes:
		// Skiff decodes each string into a fresh buffer, so the memory
		// outlives the row; downstream must treat []byte as read-only.
		if isNullable {
			return func(ac *Converter, row reflect.Value, _ int64) any {
				fv := row.Field(structIdx)
				if fv.IsNil() {
					return nil
				}
				return makeIface(typBytes, ac.abytes.put(util.UnsafeStringToBytes(fv.Elem().String())))
			}
		}
		return func(ac *Converter, row reflect.Value, _ int64) any {
			return makeIface(typBytes, ac.abytes.put(util.UnsafeStringToBytes(row.Field(structIdx).String())))
		}
	case ytschema.TypeDate:
		if isNullable {
			return func(ac *Converter, row reflect.Value, _ int64) any {
				fv := row.Field(structIdx)
				if fv.IsNil() {
					return nil
				}
				return makeIface(typTime, ac.atm.put(time.Unix(int64(fv.Elem().Uint())*86400, 0).UTC()))
			}
		}
		return func(ac *Converter, row reflect.Value, _ int64) any {
			return makeIface(typTime, ac.atm.put(time.Unix(int64(row.Field(structIdx).Uint())*86400, 0).UTC()))
		}
	case ytschema.TypeDatetime:
		if isNullable {
			return func(ac *Converter, row reflect.Value, _ int64) any {
				fv := row.Field(structIdx)
				if fv.IsNil() {
					return nil
				}
				return makeIface(typTime, ac.atm.put(time.Unix(int64(fv.Elem().Uint()), 0).UTC()))
			}
		}
		return func(ac *Converter, row reflect.Value, _ int64) any {
			return makeIface(typTime, ac.atm.put(time.Unix(int64(row.Field(structIdx).Uint()), 0).UTC()))
		}
	case ytschema.TypeTimestamp:
		if isNullable {
			return func(ac *Converter, row reflect.Value, _ int64) any {
				fv := row.Field(structIdx)
				if fv.IsNil() {
					return nil
				}
				return makeIface(typTime, ac.atm.put(time.UnixMicro(int64(fv.Elem().Uint())).UTC()))
			}
		}
		return func(ac *Converter, row reflect.Value, _ int64) any {
			return makeIface(typTime, ac.atm.put(time.UnixMicro(int64(row.Field(structIdx).Uint())).UTC()))
		}
	case ytschema.TypeInterval:
		if isNullable {
			return func(ac *Converter, row reflect.Value, _ int64) any {
				fv := row.Field(structIdx)
				if fv.IsNil() {
					return nil
				}
				return makeIface(typDuration, ac.adur.put(time.Duration(fv.Elem().Int())*time.Microsecond))
			}
		}
		return func(ac *Converter, row reflect.Value, _ int64) any {
			return makeIface(typDuration, ac.adur.put(time.Duration(row.Field(structIdx).Int())*time.Microsecond))
		}
	default:
		return anyConverter(structIdx, isNullable)
	}
}

// anyConverter handles columns whose YT type doesn't map to a primitive arena
// (TypeAny, composites, unknown). The value is already an interface{} in the
// decoded row, so no arena/boxing is needed — reflect.Value.Interface() copies
// the eface header.
func anyConverter(structIdx int, isNullable bool) cellConverter {
	if isNullable {
		return func(_ *Converter, row reflect.Value, _ int64) any {
			fv := row.Field(structIdx)
			if fv.IsNil() {
				return nil
			}
			return fv.Elem().Interface()
		}
	}
	return func(_ *Converter, row reflect.Value, _ int64) any {
		return row.Field(structIdx).Interface()
	}
}

// Clone builds a fresh Converter with the same column schema but its own
// arenas. The cellConverter slice is immutable and depends only on the schema,
// so it's shared instead of rebuilt.
func (ac *Converter) Clone() *Converter {
	cp := freshConverter(ac.chunkCap)
	cp.converters = ac.converters
	return cp
}

// Convert converts a single row (decoded by Skiff into a reflect struct) into
// a []any suitable for ChangeItem.
func (ac *Converter) Convert(row reflect.Value, rowIDX int64) ([]any, error) {
	values := make([]any, len(ac.converters))
	for i, conv := range ac.converters {
		values[i] = conv(ac, row, rowIDX)
	}
	return values, nil
}
