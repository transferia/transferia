package dataobjects

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/cypressmeta"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/ypath"
	ytgo "go.ytsaurus.tech/yt/go/yt"
)

func TestUniformPartTooManyTables(t *testing.T) {
	tbls := cypressmeta.YtNodes{}
	for i := 0; i < maxPartsPerSnapshot+1; i++ {
		tbls = append(tbls, &cypressmeta.YtNodeMeta{DataWeight: 1})
	}
	_, err := ComputePartsMapping(tbls, &yt.YtSource{DesiredPartSizeBytes: 1024}, logger.Log)
	require.ErrorContains(t, err, fmt.Sprint(maxPartsPerSnapshot))
}

// TestComputePartsMappingGlobalBudget checks that the part budget is
// distributed across ALL tables: two identically huge tables would each get
// maxPartsPerSnapshot shards under a per-table budget, so the global budget
// must cap the sum at exactly maxPartsPerSnapshot.
func TestComputePartsMappingGlobalBudget(t *testing.T) {
	tbls := cypressmeta.YtNodes{
		{DataWeight: 5_000_000_000},
		{DataWeight: 5_000_000_000},
	}
	res, err := ComputePartsMapping(tbls, &yt.YtSource{DesiredPartSizeBytes: 1}, logger.Log)
	require.NoError(t, err)
	total := 0
	for _, shards := range res {
		total += shards
	}
	require.Equal(t, maxPartsPerSnapshot, total)
}

func TestUniformPartTableWeightLessThanDesired(t *testing.T) {
	tbls := cypressmeta.YtNodes{
		{DataWeight: 1023},
		{DataWeight: 1},
	}
	res, err := ComputePartsMapping(tbls, &yt.YtSource{DesiredPartSizeBytes: 1024}, logger.Log)
	require.NoError(t, err)
	require.Equal(t, map[int]int{0: 1, 1: 1}, res)
}

func TestUniformPartTablePartedWeightLessThnDesired(t *testing.T) {
	tbls := cypressmeta.YtNodes{
		{DataWeight: 1025},
		{DataWeight: 2049},
		{DataWeight: 69420},
	}
	res, err := ComputePartsMapping(tbls, &yt.YtSource{DesiredPartSizeBytes: 1024}, logger.Log)
	require.NoError(t, err)
	require.Equal(t, map[int]int{0: 1, 1: 2, 2: 67}, res)
}

func TestFairPartUniform(t *testing.T) {
	tbls := cypressmeta.YtNodes{
		{DataWeight: 1},
		{DataWeight: 100000000000},
	}
	res, err := ComputePartsMapping(tbls, &yt.YtSource{DesiredPartSizeBytes: 1}, logger.Log)
	require.NoError(t, err)
	require.Equal(t, map[int]int{0: 1, 1: maxPartsPerSnapshot - 1}, res)
}

func TestUniformParts(t *testing.T) {
	tbls := cypressmeta.YtNodes{
		{DataWeight: 104},
		{DataWeight: 26889},
		{DataWeight: 1030000},
	}
	res, err := ComputePartsMapping(tbls, &yt.YtSource{DesiredPartSizeBytes: 1024}, logger.Log)
	require.NoError(t, err)
	require.Equal(t, map[int]int{0: 1, 1: 26, 2: 1005}, res)
}

func TestUniformPartsWithoutDesiredSize(t *testing.T) {
	tbls := cypressmeta.YtNodes{
		{DataWeight: 1024},
		{DataWeight: 2048},
		{DataWeight: 3072},
	}
	res, err := ComputePartsMapping(tbls, &yt.YtSource{DesiredPartSizeBytes: 1}, logger.Log)
	require.NoError(t, err)
	require.Equal(t, map[int]int{0: 1024, 1: 2048, 2: 3072}, res)
}

func testNodeID() *ytgo.NodeID {
	nid := ytgo.NodeID(guid.New())
	return &nid
}

func TestNewPartFromPartition(t *testing.T) {
	nodeID := testNodeID()
	partition := ytgo.TablePartition{
		Cookie: []byte("cookie"),
		TableRanges: []ypath.Rich{
			*ypath.NewRich("//tmp/t").AddRange(ypath.Interval(ypath.RowIndex(0), ypath.RowIndex(75001))),
			*ypath.NewRich("//tmp/t").AddRange(ypath.Interval(ypath.RowIndex(75001), ypath.RowIndex(150000))),
		},
		AggregateStatistics: ytgo.PartitionStatistics{
			RowCount: 150000,
		},
	}

	part, err := NewPartFromPartition("t", *nodeID, partition, ytgo.TxID{})
	require.NoError(t, err)
	tablePart, err := part.ToTablePart()
	require.NoError(t, err)
	require.Equal(t, "rows=[0:75001,75001:150000]", string(tablePart.Filter))
	require.Equal(t, []byte("cookie"), tablePart.GetPayload())
	require.Equal(t, uint64(150000), tablePart.EtaRow)
	require.Equal(t, uint64(0), tablePart.Offset)

	ranges, err := FilterToRanges(tablePart.Filter)
	require.NoError(t, err)
	require.Len(t, ranges, 2)
	require.Equal(t, int64(0), *ranges[0].Lower.RowIndex)
	require.Equal(t, int64(75001), *ranges[0].Upper.RowIndex)
	require.Equal(t, int64(75001), *ranges[1].Lower.RowIndex)
	require.Equal(t, int64(150000), *ranges[1].Upper.RowIndex)
}

// At the table-count limit there is exactly one budget slot per table, even
// when one table contains nearly all the data and requests many more parts.
func TestComputePartsMappingMaxTables(t *testing.T) {
	const tableCount = 100_000
	tables := make(cypressmeta.YtNodes, tableCount)
	for i := range tables {
		tables[i] = &cypressmeta.YtNodeMeta{DataWeight: 1}
	}
	tables[0].DataWeight = 1_000_000_000_000
	require.NoError(t, CheckTableCountLimit(tables))
	mapping, err := ComputePartsMapping(tables, &yt.YtSource{DesiredPartSizeBytes: 1}, nil)
	require.NoError(t, err)
	require.Len(t, mapping, tableCount)
	for i := range tables {
		require.Equal(t, 1, mapping[i], "table %d must retain its budget slot", i)
	}

	tables = append(tables, &cypressmeta.YtNodeMeta{DataWeight: 1})
	require.ErrorContains(t, CheckTableCountLimit(tables), "100000")
}

type partitionOptionsTx struct {
	ytgo.Tx
	options *ytgo.PartitionTablesOptions
}

func (tx *partitionOptionsTx) PartitionTables(_ context.Context, _ []ypath.YPath, options *ytgo.PartitionTablesOptions) (ytgo.MultiTablePartitions, error) {
	tx.options = options
	return ytgo.MultiTablePartitions{}, nil
}

// MaxPartitionCount must carry the computed per-table limit, rather than
// requesting the whole global budget for every table.
func TestBuildPartsForTableUsesComputedLimit(t *testing.T) {
	const desiredSize = 1024
	for _, tc := range []struct {
		name      string
		weight    int64
		wantParts int
	}{
		{name: "small table", weight: 1, wantParts: 1},
		{name: "desired size", weight: 3 * desiredSize, wantParts: 3},
		{name: "global cap", weight: 2 * 100_000 * desiredSize, wantParts: 100_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := &cypressmeta.YtNodeMeta{DataWeight: tc.weight, NodeID: testNodeID()}
			mapping, err := ComputePartsMapping(cypressmeta.YtNodes{table}, &yt.YtSource{DesiredPartSizeBytes: desiredSize}, nil)
			require.NoError(t, err)
			require.Equal(t, tc.wantParts, mapping[0])

			tx := &partitionOptionsTx{}
			_, err = BuildPartsForTable(context.Background(), tx, ytgo.TxID{}, table, mapping[0], desiredSize, nil)
			require.NoError(t, err)
			require.NotNil(t, tx.options)
			require.NotNil(t, tx.options.MaxPartitionCount)
			require.Equal(t, tc.wantParts, *tx.options.MaxPartitionCount)
			require.EqualValues(t, desiredSize, tx.options.DataWeightPerPartition)
		})
	}
}

func TestComputePartsMappingRejectsWeightOverflow(t *testing.T) {
	// A final sign check misses this overflow: the wrapped sum is positive.
	tables := cypressmeta.YtNodes{
		{DataWeight: math.MaxInt64},
		{DataWeight: math.MaxInt64},
		{DataWeight: 3},
	}
	_, err := ComputePartsMapping(tables, &yt.YtSource{DesiredPartSizeBytes: 1}, nil)
	require.ErrorIs(t, err, tablesWeightOverflowErr)
}
