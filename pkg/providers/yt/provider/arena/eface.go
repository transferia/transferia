package arena

import (
	"time"
	"unsafe"
)

// eface mirrors the runtime layout of interface{}. Assembling one manually lets
// us box primitive values without triggering runtime.convT* heap allocations
// (~40% of all objects in the snapshot read profile). Layout is validated at
// init() by round-tripping several concrete types.
type eface struct {
	typ  unsafe.Pointer
	data unsafe.Pointer
}

// extractType returns the type descriptor pointer of the value inside v. Type
// descriptors live in the binary's read-only data segment and are stable for
// the process lifetime.
func extractType(v any) unsafe.Pointer {
	return (*[2]unsafe.Pointer)(unsafe.Pointer(&v))[0]
}

var (
	typInt8     = extractType(int8(0))
	typInt16    = extractType(int16(0))
	typInt32    = extractType(int32(0))
	typInt64    = extractType(int64(0))
	typUint8    = extractType(uint8(0))
	typUint16   = extractType(uint16(0))
	typUint32   = extractType(uint32(0))
	typUint64   = extractType(uint64(0))
	typFloat32  = extractType(float32(0))
	typFloat64  = extractType(float64(0))
	typBool     = extractType(false)
	typString   = extractType("")
	typBytes    = extractType([]byte(nil))
	typTime     = extractType(time.Time{})
	typDuration = extractType(time.Duration(0))
)

// makeIface builds an interface{} with the given type descriptor and pointer to
// the value. The memory pointed to by data must outlive every reader of the
// returned interface — enforced here by keeping the arena alive as a field of
// the owning Converter.
func makeIface(typ, data unsafe.Pointer) any {
	var i any
	ep := (*eface)(unsafe.Pointer(&i))
	ep.typ = typ
	ep.data = data
	return i
}

func init() {
	var i64 int64 = 1 << 40
	if v, ok := makeIface(typInt64, unsafe.Pointer(&i64)).(int64); !ok || v != 1<<40 {
		panic("yt/provider/arena: eface layout smoke check failed for int64")
	}
	s := "eface-smoke"
	if v, ok := makeIface(typString, unsafe.Pointer(&s)).(string); !ok || v != s {
		panic("yt/provider/arena: eface layout smoke check failed for string")
	}
	tm := time.Unix(12345, 6789).UTC()
	if v, ok := makeIface(typTime, unsafe.Pointer(&tm)).(time.Time); !ok || !v.Equal(tm) {
		panic("yt/provider/arena: eface layout smoke check failed for time.Time")
	}
}
