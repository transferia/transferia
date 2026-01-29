# Serializer

This package defines serializers interfaces, that serializes ChangeItems to []bytes

Supported Formats:
 * csv
 * json
 * parquet
 * raw

## Batch Serializer

Batch serializer provides efficient parallel serialization of multiple [`ChangeItem`](../../pkg/abstract/changeitem.go) objects with automatic concurrency management and memory optimization.

### Architecture

The batch serializer consists of several key components:

1. **[`BatchSerializer`](interface.go:17)** interface - defines methods for batch serialization
2. **[`batchSerializer`](batch.go:28)** implementation - core logic with concurrency support
3. **[`BufferPool`](buffer/pool.go:8)** - manages reusable byte buffers to reduce allocations

```
┌─────────────────────────────────────────────────────────────┐
│                    BatchSerializer                          │
│                                                             │
│  ┌──────────────┐      ┌──────────────┐                     │
│  │  Serialize() │      │SerializeAnd  │                     │
│  │              │      │   Write()    │                     │
│  └──────┬───────┘      └──────┬───────┘                     │
│         │                     │                             │
│         └─────────┬───────────┘                             │
│                   ▼                                         │
│         ┌─────────────────────┐                             │
│         │  batchSerializer    │                             │
│         │  ┌───────────────┐  │                             │
│         │  │ serializer    │  │  (JSON/CSV/Parquet/Raw)     │
│         │  │ separator     │  │                             │
│         │  │ concurrency   │  │                             │
│         │  │ threshold     │  │                             │
│         │  │ bufferPool    │◄─┼─────┐                       │
│         │  └───────────────┘  │     │                       │
│         └─────────────────────┘     │                       │
│                                     │                       │
│         ┌───────────────────────────┘                       │
│         ▼                                                   │
│  ┌──────────────────┐                                       │
│  │   BufferPool     │                                       │
│  │ ┌──────────────┐ │                                       │
│  │ │ Buffer #1    │ │                                       │
│  │ │ Buffer #2    │ │  (Size = concurrency)                 │
│  │ │ Buffer #3    │ │                                       │
│  │ │    ...       │ │                                       │
│  │ └──────────────┘ │                                       │
│  └──────────────────┘                                       │
└─────────────────────────────────────────────────────────────┘
```

### Key Features

#### 1. Automatic Concurrency

The serializer automatically determines optimal concurrency based on:
- **Threshold**: Default 25,000 items ([`DefaultBatchSerializerThreshold`](batch.go:18))
- **Concurrency level**: Defaults to `runtime.GOMAXPROCS(0)` if not specified
- **Batch size**: Items are split into chunks of `threshold` size

#### 2. Two Serialization Modes

##### Sequential Mode (< threshold items or concurrency disabled)
```go
// Used when: len(items) <= threshold OR DisableConcurrency == true
data, err := serializer.Serialize(items)
```
- Single-threaded execution
- Direct serialization without chunking
- Lower overhead for small batches

```
Input data (< 25000 items)
┌────────────────────────────────┐
│ [Item1, Item2, ..., ItemN]     │
└────────────┬───────────────────┘
             │
             ▼
      ┌──────────────┐
      │  Serialize   │  (Single goroutine)
      └──────┬───────┘
             │
             ▼
┌────────────────────────────────┐
│  Result: []byte                │
└────────────────────────────────┘
```

##### Parallel Mode (≥ threshold items)
```go
// Automatically splits items into chunks
// Each chunk is serialized in parallel
// Results are joined with separator
```
- Items split into chunks of `threshold` size
- Each chunk processed by separate goroutine
- Limited by `concurrency` parameter via [`errgroup.SetLimit()`](batch.go:85)
- Results joined with separator bytes

```
Input data (≥ 25000 items)
┌─────────────────────────────────────────────────────────────┐
│ [Item1, Item2, ..., Item75000]                              │
└────────────┬────────────────────────────────────────────────┘
             │
             │ Split into chunks (threshold = 25000)
             ▼
    ┌────────┴────────┬────────────────┐
    │                 │                │
    ▼                 ▼                ▼
┌─────────┐      ┌─────────┐      ┌─────────┐
│ Chunk 1 │      │ Chunk 2 │      │ Chunk 3 │
│ [0:25K] │      │[25K:50K]│      │[50K:75K]│
└────┬────┘      └────┬────┘      └────┬────┘
     │                │                │
     │ Goroutine 1    │ Goroutine 2    │ Goroutine 3
     ▼                ▼                ▼
┌─────────┐      ┌─────────┐      ┌─────────┐
│Buffer #1│      │Buffer #2│      │Buffer #3│  ◄── BufferPool
│Serialize│      │Serialize│      │Serialize│
└────┬────┘      └────┬────┘      └────┬────┘
     │                │                │
     │                │                │
     └────────┬───────┴────────┬───────┘
              │                │
              ▼                ▼
         ┌────────────────────────┐
         │  bytes.Join(separator) │
         └───────────┬────────────┘
                     │
                     ▼
         ┌────────────────────────┐
         │  Result: []byte        │
         └────────────────────────┘
```

#### 3. Memory Optimization

**Buffer Pool** ([`buffer.BufferPool`](buffer/pool.go:8)):
- Pre-allocated pool of `bytes.Buffer` objects
- Size equals concurrency level
- Buffers are reset and reused via [`Get()`](buffer/pool.go:32) and [`Put()`](buffer/pool.go:43)

**Benefits**:
- Reduces GC pressure
- Eliminates repeated allocations
- Improves throughput for large batches

```
Buffer lifecycle:

┌──────────────────────────────────────────────────────────┐
│                    BufferPool                            │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐      │
│  │Buffer #1│  │Buffer #2│  │Buffer #3│  │Buffer #4│      │
│  └────┬────┘  └─────────┘  └─────────┘  └─────────┘      │
└───────┼──────────────────────────────────────────────────┘
        │
        │ Get(ctx) - get buffer
        ▼
   ┌─────────┐
   │Goroutine│
   │  ┌───┐  │
   │  │buf│  │  1. buf.Reset() - clear
   │  └─┬─┘  │  2. Serialize data
   │    │    │  3. Write result
   │    │    │
   └────┼────┘
        │
        │ Put(ctx, buf) - return buffer
        ▼
┌──────────────────────────────────────────────────────────┐
│                    BufferPool                            │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐      │
│  │Buffer #1│  │Buffer #2│  │Buffer #3│  │Buffer #4│      │
│  └─────────┘  └─────────┘  └─────────┘  └─────────┘      │
└──────────────────────────────────────────────────────────┘
     ▲
     └─ Buffer ready for reuse
```

#### 4. Streaming API

The [`SerializeAndWrite()`](batch.go:119) method provides streaming serialization:

```go
err := serializer.SerializeAndWrite(ctx, items, writer)
```

**Features**:
- Writes directly to `io.Writer` without buffering entire result
- Maintains order: uses condition variable to ensure sequential writes
- Each goroutine:
  1. Gets buffer from pool
  2. Serializes its chunk
  3. Waits for its turn (via [`sync.Cond`](batch.go:138))
  4. Writes to output
  5. Returns buffer to pool

**Synchronization**:
```go
// Wait for previous chunk to be written
for nextToWrite != i && egCtx.Err() == nil {
    cond.Wait()
}
// Write current chunk
writer.Write(buf.Bytes())
nextToWrite++
cond.Broadcast()
```

```
Streaming write with order preservation:

Goroutine 1         Goroutine 2         Goroutine 3
    │                   │                   │
    ▼                   ▼                   ▼
┌─────────┐        ┌─────────┐        ┌─────────┐
│Serialize│        │Serialize│        │Serialize│
│ Chunk 1 │        │ Chunk 2 │        │ Chunk 3 │
└────┬────┘        └────┬────┘        └────┬────┘
     │                  │                  │
     │ Ready            │ Ready            │ Ready
     ▼                  ▼                  ▼
┌─────────┐        ┌─────────┐        ┌─────────┐
│ Wait    │        │ Wait    │        │ Wait    │
│ turn=0  │        │ turn=1  │        │ turn=2  │
└────┬────┘        └────┬────┘        └────┬────┘
     │                  │                  │
     │ nextToWrite=0    │ Waiting...       │ Waiting...
     ▼                  │                  │
┌─────────┐             │                  │
│ Write   │             │                  │
│ Chunk 1 │             │                  │
└────┬────┘             │                  │
     │                  │                  │
     │ nextToWrite=1    │                  │
     │ Broadcast()      │                  │
     │                  ▼                  │
     │             ┌─────────┐             │
     │             │ Write   │             │
     │             │ Chunk 2 │             │
     │             └────┬────┘             │
     │                  │                  │
     │                  │ nextToWrite=2    │
     │                  │ Broadcast()      │
     │                  │                  ▼
     │                  │             ┌─────────┐
     │                  │             │ Write   │
     │                  │             │ Chunk 3 │
     │                  │             └────┬────┘
     │                  │                  │
     ▼                  ▼                  ▼
                  io.Writer
     [Chunk 1][Chunk 2][Chunk 3]  ◄── Order preserved!
```

### Configuration

#### [`BatchSerializerConfig`](batch.go:21)
```go
type BatchSerializerConfig struct {
    Concurrency        int  // Number of parallel workers (default: GOMAXPROCS)
    Threshold          int  // Min items for parallel mode (default: 25000)
    DisableConcurrency bool // Force sequential mode
}
```

#### [`BatchSerializerCommonConfig`](batch_factory.go:10)
```go
type BatchSerializerCommonConfig struct {
    Format               model.ParsingFormat      // Output format (JSON/CSV/Parquet/Raw)
    CompressionCodec     compress.Codec           // For Parquet format
    UnsupportedItemKinds map[abstract.Kind]bool   // Filter item types
    AddClosingNewLine    bool                     // Add newline after each item
    AnyAsString          bool                     // Serialize any type as string
}
```

### Implementation Details

1. **Chunking** ([`Serialize()`](batch.go:73)):
   - Calculates number of chunks: `(len(items) + threshold - 1) / threshold`
   - Each chunk: `items[i*threshold : min((i+1)*threshold, len(items))]`

2. **Separator Handling**:
   - Applied between chunks during join
   - Last separator trimmed via [`bytes.TrimSuffix()`](batch.go:114)
   - For streaming: separator added to all chunks except last

3. **Error Handling**:
   - Any goroutine error cancels all others via `errgroup`
   - Context cancellation propagates immediately
   - Partial results are discarded on error
