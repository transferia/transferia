package serializer

import (
	"bytes"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func makeParquetItems(n int, valueSize int) ([]*abstract.ChangeItem, *abstract.TableSchema) {
	cols := []abstract.ColSchema{
		{
			TableName:  "test",
			ColumnName: "id",
			DataType:   string(ytschema.TypeInt64),
			Required:   true,
		},
		{
			TableName:  "test",
			ColumnName: "payload",
			DataType:   string(ytschema.TypeString),
			Required:   false,
		},
	}
	tableSchema := abstract.NewTableSchema(cols)
	payload := strings.Repeat("x", valueSize)

	items := make([]*abstract.ChangeItem, n)
	for i := range items {
		items[i] = &abstract.ChangeItem{
			Kind:         abstract.InsertKind,
			TableSchema:  tableSchema,
			ColumnNames:  []string{"id", "payload"},
			ColumnValues: []interface{}{int64(i), payload},
			Size:         abstract.EventSize{Read: uint64(valueSize), Values: util.DeepSizeof([]interface{}{int64(i), payload})},
		}
	}
	return items, tableSchema
}

func writtenRowGroupsCount(t *testing.T, buf *bytes.Buffer) int {
	pqFile, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	return len(pqFile.RowGroups())
}

// TestParquetBatchSerializer_RowGroupFlushing verifies that row groups are
// flushed to the underlying writer incrementally (before Close), which is the
// property that prevents memory from growing proportionally to file size.
func TestParquetBatchSerializer_RowGroupFlushing(t *testing.T) {
	const (
		rowGroupMaxRows = 1_000
		itemValueSize   = 1_024 // 1 KB per row
		totalItems      = 5 * rowGroupMaxRows
	)

	items, _ := makeParquetItems(totalItems, itemValueSize)

	s := &parquetBatchSerializer{
		rowGroupMaxRows: rowGroupMaxRows,
	}

	serialized, err := s.Serialize(items)
	require.NoError(t, err)

	// With MaxRowsPerRowGroup, completed row groups must be returned by Serialize
	// before Close() — at least 4 complete row groups here.
	require.Greater(t, len(serialized), 0,
		"no data returned by Serialize(): row groups are not being flushed incrementally")

	var buf bytes.Buffer
	buf.Write(serialized)

	final, err := s.Close()
	require.NoError(t, err)

	// Close() must return the final (partial) row group and footer.
	require.Greater(t, len(final), 0,
		"Close() must return the final row group and file footer")

	buf.Write(final)

	require.Equal(t, totalItems/rowGroupMaxRows, writtenRowGroupsCount(t, &buf),
		"expected exactly %d row groups (%d rows / %d rows-per-group)",
		totalItems/rowGroupMaxRows, totalItems, rowGroupMaxRows)
}

// TestParquetBatchSerializer_MemoryBounded verifies that heap memory does not
// grow proportionally to the total amount of data written. After flushing N row
// groups and forcing GC, writing N more groups should not significantly increase
// live heap — the flushed row groups' buffers should have been reclaimed.
func TestParquetBatchSerializer_MemoryBounded(t *testing.T) {
	const (
		rowGroupMaxRows = 1_000
		itemValueSize   = 1_024 // 1 KB per row
		rowsPerBatch    = 500
		batchesPerPhase = 6 // 3 complete row groups per phase (6 * 500 / 1000 = 3)
	)

	s := &parquetBatchSerializer{
		rowGroupMaxRows: rowGroupMaxRows,
	}

	writeBatches := func(n int) {
		batch, _ := makeParquetItems(rowsPerBatch, itemValueSize)
		for i := 0; i < n; i++ {
			_, err := s.Serialize(batch)
			require.NoError(t, err)
		}
	}

	// Disable GC during measurement so we observe allocations, not GC timing.
	old := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(old)

	// Phase 1: write 3 row groups.
	writeBatches(batchesPerPhase)
	runtime.GC()
	var after1 runtime.MemStats
	runtime.ReadMemStats(&after1)

	// Phase 2: write 3 more row groups.
	writeBatches(batchesPerPhase)
	runtime.GC()
	var after2 runtime.MemStats
	runtime.ReadMemStats(&after2)

	// With proper flushing the flushed row-group buffers are freed by GC.
	// HeapAlloc growth between phases should be small compared to the data written
	// per phase (~3 MB uncompressed). Allow 2× a single row group as headroom.
	oneRowGroupBytes := int64(rowGroupMaxRows) * int64(itemValueSize)
	heapGrowth := int64(after2.HeapAlloc) - int64(after1.HeapAlloc)
	require.Less(t, heapGrowth, 2*oneRowGroupBytes,
		"heap grew by %d bytes between phases; expected < %d (2× one row group). "+
			"Possible memory regression: row groups may not be flushed incrementally.",
		heapGrowth, 2*oneRowGroupBytes,
	)
}

// TestParquetBatchSerializer_RowGroupMaxBytes verifies that row groups are
// flushed based on estimated byte size when RowGroupMaxBytes is configured.
func TestParquetBatchSerializer_RowGroupMaxBytes(t *testing.T) {
	const (
		rowGroupMaxRows  = 1_000
		itemValueSize    = 1_024         // 1 KB string payload per row
		totalItems       = 5_000         // ~5 MB uncompressed total
		rowGroupMaxBytes = 1_024 * 1_024 // 1 MB,  expect ~5 flushes
	)

	items, _ := makeParquetItems(totalItems, itemValueSize)

	s := &parquetBatchSerializer{
		rowGroupMaxRows:  rowGroupMaxRows,
		rowGroupMaxBytes: rowGroupMaxBytes,
	}

	serialized, err := s.Serialize(items)
	require.NoError(t, err)
	require.Greater(t, len(serialized), 0,
		"no data returned by Serialize(): row groups are not being flushed incrementally")

	var buf bytes.Buffer
	buf.Write(serialized)

	final, err := s.Close()
	require.NoError(t, err)
	require.Greater(t, len(final), 0,
		"Close() must return the final row group and file footer")
	buf.Write(final)

	rowGroups := writtenRowGroupsCount(t, &buf)
	require.Greater(t, rowGroups, 1,
		"expected multiple row groups with RowGroupMaxBytes=%d, got %d",
		rowGroupMaxBytes, rowGroups)
}
