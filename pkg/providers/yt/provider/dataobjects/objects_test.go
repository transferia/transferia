package dataobjects

import (
	"fmt"
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
	for i := 0; i < 1025; i++ {
		tbls = append(tbls, &cypressmeta.YtNodeMeta{DataWeight: 1})
	}
	_, err := ComputePartsMapping(tbls, &yt.YtSource{DesiredPartSizeBytes: 1024}, logger.Log)
	require.ErrorContains(t, err, fmt.Sprint(grpcShardLimit))
}

// TestComputePartsMappingGlobalBudget checks that the 1024-part budget is
// distributed across ALL tables: two identically huge tables would each get
// ~1024 shards under a per-table budget (2048 in total), so the global budget
// must cap the sum at exactly grpcShardLimit.
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
	require.Equal(t, grpcShardLimit, total)
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
	require.Equal(t, map[int]int{0: 1, 1: 1023}, res)
}

func TestUniformParts(t *testing.T) {
	tbls := cypressmeta.YtNodes{
		{DataWeight: 104},
		{DataWeight: 26889},
		{DataWeight: 1030000},
	}
	res, err := ComputePartsMapping(tbls, &yt.YtSource{DesiredPartSizeBytes: 1024}, logger.Log)
	require.NoError(t, err)
	require.Equal(t, map[int]int{0: 1, 1: 26, 2: 997}, res)
}

func TestUniformPartsWithoutDesiredSize(t *testing.T) {
	tbls := cypressmeta.YtNodes{
		{DataWeight: 1024},
		{DataWeight: 2048},
		{DataWeight: 3072},
	}
	res, err := ComputePartsMapping(tbls, &yt.YtSource{DesiredPartSizeBytes: 1}, logger.Log)
	require.NoError(t, err)
	require.Equal(t, map[int]int{0: 170, 1: 341, 2: 513}, res)
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
