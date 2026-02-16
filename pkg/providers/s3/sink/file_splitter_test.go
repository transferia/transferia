package sink

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	s3_provider "github.com/transferia/transferia/pkg/providers/s3"
)

func TestNewS3ObjectRefFullKey(t *testing.T) {
	ref := newS3ObjectRef(
		"",
		"my_schema",
		"events",
		"part42",
		"1700000000",
		model.ParsingFormatJSONLine,
		s3_provider.GzipEncoding,
	)

	got := ref.fullKey(0)
	expectedHash := hashPartID("part42")
	want := "my_schema/events/part-1700000000-" + expectedHash + ".00000.jsonl.gz"
	require.Equal(t, want, got)

	got2 := ref.fullKey(5)
	want2 := "my_schema/events/part-1700000000-" + expectedHash + ".00005.jsonl.gz"
	require.Equal(t, want2, got2)
}

func TestNewS3ObjectRefFullKeyWithLayout(t *testing.T) {
	ref := newS3ObjectRef(
		"snapshot-2024",
		"my_schema",
		"events",
		"part42",
		"1700000000",
		model.ParsingFormatJSONLine,
		s3_provider.GzipEncoding,
	)

	got := ref.fullKey(0)
	expectedHash := hashPartID("part42")
	want := "snapshot-2024/my_schema/events/part-1700000000-" + expectedHash + ".00000.jsonl.gz"
	require.Equal(t, want, got)
}

func TestNewS3ObjectRefNoNamespace(t *testing.T) {
	ref := newS3ObjectRef(
		"",
		"",
		"events",
		"",
		"1700000000",
		model.ParsingFormatJSON,
		s3_provider.NoEncoding,
	)

	got := ref.fullKey(0)
	expectedHash := hashPartID("")
	want := "events/part-1700000000-" + expectedHash + ".00000.json"
	require.Equal(t, want, got)
}

func TestFileSplitterUnlimited(t *testing.T) {
	splitter := newFileSplitter(0, 0)
	ref := newS3ObjectRef("", "", "table", "", "1700000000", model.ParsingFormatJSON, s3_provider.NoEncoding)

	increased := splitter.increaseKey(ref)
	expectedHash := hashPartID("")
	require.Equal(t, "table/part-1700000000-"+expectedHash+".00000.json", increased)

	items := makeTestItems(5, 100)
	added := splitter.addItems(ref, items)
	require.Equal(t, 5, added)

	// No limits - should not add any items to counter
	require.Equal(t, 0, splitter.rowsByRef[ref])
	require.Equal(t, 0, splitter.bytesByRef[ref])

	emptyAdded := splitter.addItems(ref, nil)
	require.Equal(t, 0, emptyAdded)
}

func TestFileSplitterRotationByRows(t *testing.T) {
	splitter := newFileSplitter(2, 0)
	ref := newS3ObjectRef("", "ns", "orders", "", "1700000000", model.ParsingFormatJSON, s3_provider.NoEncoding)

	first := splitter.increaseKey(ref)
	expectedHash := hashPartID("")
	require.Equal(t, "ns/orders/part-1700000000-"+expectedHash+".00000.json", first)

	number := splitter.keyNumber(ref)
	require.Equal(t, 0, number)

	items := makeTestItems(2, 100)
	added := splitter.addItems(ref, items)
	require.Equal(t, 2, added)

	// File is full by rows, should not add more
	moreItems := makeTestItems(1, 100)
	added = splitter.addItems(ref, moreItems)
	require.Equal(t, 0, added)

	second := splitter.increaseKey(ref)
	require.Equal(t, "ns/orders/part-1700000000-"+expectedHash+".00001.json", second)

	number = splitter.keyNumber(ref)
	require.Equal(t, 1, number)

	require.Equal(t, 0, splitter.rowsByRef[ref])
	require.Equal(t, 0, splitter.bytesByRef[ref])

	resolved := splitter.key(ref)
	require.Equal(t, second, resolved)

	third := splitter.increaseKey(ref)
	require.Equal(t, "ns/orders/part-1700000000-"+expectedHash+".00002.json", third)

	number = splitter.keyNumber(ref)
	require.Equal(t, 2, number)
}

func TestFileSplitterRotationByBytes(t *testing.T) {
	splitter := newFileSplitter(0, 250)
	ref := newS3ObjectRef("", "", "data", "", "1700000000", model.ParsingFormatCSV, s3_provider.NoEncoding)

	splitter.increaseKey(ref)

	items := makeTestItems(3, 100) // 3 items x 100 bytes = 300 bytes
	added := splitter.addItems(ref, items)
	// Should add first 2 items (200 bytes), third would exceed 250 limit
	// Actually: first item (0+100=100 <= 250, ok), second (100+100=200 <= 250, ok), third (200+100=300 > 250, stop)
	// But the byte check only triggers when rowsByRef > 0, so:
	// item1: rowsByRef=0, no byte check -> add (rows=1, bytes=100)
	// item2: rowsByRef=1>0, 100+100=200 <= 250 -> add (rows=2, bytes=200)
	// item3: rowsByRef=2>0, 200+100=300 > 250 -> stop
	require.Equal(t, 2, added)

	// Should not add more, byte limit reached
	more := makeTestItems(1, 100)
	added = splitter.addItems(ref, more)
	require.Equal(t, 0, added)

	// Rotate and add
	splitter.increaseKey(ref)
	added = splitter.addItems(ref, more)
	require.Equal(t, 1, added)
}

func TestFileSplitterRotationByBothLimits(t *testing.T) {
	// Row limit: 3, Byte limit: 250
	splitter := newFileSplitter(3, 250)
	ref := newS3ObjectRef("", "", "data", "", "1700000000", model.ParsingFormatCSV, s3_provider.NoEncoding)

	splitter.increaseKey(ref)

	// Items of 100 bytes each. Byte limit (250) would be reached before row limit (3)
	items := makeTestItems(3, 100)
	added := splitter.addItems(ref, items)
	require.Equal(t, 2, added) // Byte limit stops at 2 items (200 bytes, 3rd would be 300 > 250)

	splitter.increaseKey(ref)

	// Items of 10 bytes each. Row limit (3) would be reached before byte limit (250)
	smallItems := makeTestItems(5, 10)
	added = splitter.addItems(ref, smallItems)
	require.Equal(t, 3, added) // Row limit stops at 3 items
}

func TestFileSplitterAddItemsToUnknownRef(t *testing.T) {
	splitter := newFileSplitter(0, 0)
	ref := newS3ObjectRef("", "", "orders", "", "1700000000", model.ParsingFormatJSON, s3_provider.NoEncoding)

	items := makeTestItems(1, 100)
	added := splitter.addItems(ref, items)
	require.Equal(t, 0, added)
}

func TestFileSplitterOversizedSingleItem(t *testing.T) {
	splitter := newFileSplitter(0, 100)
	ref := newS3ObjectRef("", "", "data", "", "1700000000", model.ParsingFormatCSV, s3_provider.NoEncoding)

	splitter.increaseKey(ref)

	// Single item larger than byte limit — must still be accepted
	items := makeTestItems(1, 500)
	added := splitter.addItems(ref, items)
	require.Equal(t, 1, added)

	// Next item should NOT be added (already over limit)
	more := makeTestItems(1, 10)
	added = splitter.addItems(ref, more)
	require.Equal(t, 0, added)
}

func TestHashPartID(t *testing.T) {
	// Same input always produces same hash
	h1 := hashPartID("part42")
	h2 := hashPartID("part42")
	require.Equal(t, h1, h2)

	// Different inputs produce different hashes
	h3 := hashPartID("part43")
	require.NotEqual(t, h1, h3)

	// Empty partID uses "default"
	h4 := hashPartID("")
	require.Len(t, h4, partIDHashLen)

	// All hashes have the correct length
	require.Len(t, h1, partIDHashLen)
	require.Len(t, h3, partIDHashLen)
}

// makeTestItems creates test ChangeItems with a given Size.Read value
func makeTestItems(count int, readBytes uint64) []*abstract.ChangeItem {
	items := make([]*abstract.ChangeItem, count)
	for i := 0; i < count; i++ {
		items[i] = &abstract.ChangeItem{
			Kind: abstract.InsertKind,
			Size: abstract.EventSize{Read: readBytes},
		}
	}
	return items
}
