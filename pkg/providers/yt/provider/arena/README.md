# Arena boxing for the YT snapshot read path

Replaces per-cell `runtime.convT*` allocations on the primitive-schema
Skiff decode path with arena-boxed values behind a manually forged
`interface{}` layout. Downstream sees the same concrete Go types
(`int8`, `string`, `time.Time`, ...), so `ChangeItem` contracts are
unchanged.

## Why forged interfaces

In plain Go, returning a primitive at an `interface{}` position causes
the compiler to insert a `runtime.convT*` call (`convT16`, `convT32`,
`convTstring`, ...). That helper does `mallocgc(sizeof(T), *_type)` and
copies the value onto the heap — one small heap allocation per cell.

We short-circuit that: an `interface{}` is really two words —
`(*_type, unsafe.Pointer)` — and we build one manually. The type
descriptor is a compile-time constant that lives in the binary's
read-only data segment; the data pointer we make ourselves, into a
per-type arena we control.

## Files

- `eface.go` — mirrored layout of `interface{}` (`eface`,
  `extractType`, `makeIface`) plus an `init()` smoke check that panics
  loudly if a future Go toolchain changes that layout.
  - `extractType(v any) unsafe.Pointer` reads the `*_type` word out of
    a real `any`; used once at package init to cache the descriptor of
    each Go primitive we care about (`typInt8`, `typString`,
    `typTime`, ...).
  - `makeIface(typ, data unsafe.Pointer) any` writes those two words
    directly into a new `any` and returns it — no `runtime.convT*`,
    no `mallocgc`.

- `arena.go` — generic chunked slot buffer `arena[T]`. One arena per
  concrete Go type the read path can produce (int8..int64,
  uint8..uint64, float32/64, bool, string, []byte, time.Time,
  time.Duration). When the current chunk fills, a fresh chunk is
  allocated; old chunks stay alive as long as any forged `interface{}`
  still points at one of their slots. GC reclaims each chunk once its
  slots become unreachable downstream. Using `[]T` (not `[]byte`) is
  important — GC needs correctly-typed backing storage to keep string
  headers, `*Location` inside `time.Time`, etc. reachable.

- `converter.go` — the schema-facing API. `NewConverter` builds a
  `cellConverter` closure per column, specialized to
  `(ytType, isNullable, structIdx)`. Nullable columns pay one `IsNil`
  check per cell; non-nullable columns skip it entirely. The synthetic
  row-index column and the any-fallback (for `TypeAny` / composites /
  anything yson-decoded as `interface{}`) are also baked into the
  closure at setup, so the hot loop is a plain range over
  `converters` with no per-cell branching:

  ```go
  for i, conv := range ac.converters {
      values[i] = conv(ac, row, rowIDX)
  }
  ```

  `Clone` shares the `converters` slice — closures depend only on the
  schema, not on the arena instance, so per-reader clones don't rebuild
  them.

## Bench

`bench_test.go` runs `Convert` on 10 / 25 / 70 column schemas (the
last one mirrors a real medium-sized YT-to-CH transfer). One `[]any`
allocation per row remains on the hot path — the returned slice
itself.

```
CGO_ENABLED=0 go test -bench=BenchmarkConverter -benchmem \
    ./pkg/providers/yt/provider/arena/...
```

## Background

See [TM-10308](https://st.yandex-team.ru/TM-10308) for the motivating
profile, before/after throughput numbers, and design discussion.
