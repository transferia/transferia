//go:build !disable_yt_provider

package dataobjects

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/tablemeta"
)

func TestUniformPartTooManyTables(t *testing.T) {
	dataObjs := &YTDataObjects{
		tbls: []*tablemeta.YtTableMeta{},
		cfg:  &yt.YtSource{DesiredPartSizeBytes: 1024},
		lgr:  logger.Log,
	}
	for i := 0; i < 1025; i++ {
		dataObjs.tbls = append(dataObjs.tbls, &tablemeta.YtTableMeta{DataWeight: 1})
	}
	_, err := dataObjs.uniformParts()
	require.ErrorContains(t, err, fmt.Sprint(rune(grpcShardLimit)))
}

func TestUniformPartTableWeightLessThanDesired(t *testing.T) {
	dataObjs := &YTDataObjects{
		tbls: []*tablemeta.YtTableMeta{
			{
				DataWeight: 1023,
			},
			{
				DataWeight: 1,
			},
		},
		cfg: &yt.YtSource{DesiredPartSizeBytes: 1024},
		lgr: logger.Log,
	}
	res, err := dataObjs.uniformParts()
	require.NoError(t, err)
	require.Equal(t, map[int]int{0: 1, 1: 1}, res)
}

func TestUniformPartTablePartedWeightLessThnDesired(t *testing.T) {
	dataObjs := &YTDataObjects{
		tbls: []*tablemeta.YtTableMeta{
			{
				DataWeight: 1025,
			},
			{
				DataWeight: 2049,
			},
			{
				DataWeight: 69420,
			},
		},
		cfg: &yt.YtSource{DesiredPartSizeBytes: 1024},
		lgr: logger.Log,
	}
	res, err := dataObjs.uniformParts()
	require.NoError(t, err)
	require.Equal(t, map[int]int{0: 1, 1: 2, 2: 67}, res)
}

func TestFairPartUniform(t *testing.T) {
	dataObjs := &YTDataObjects{
		tbls: []*tablemeta.YtTableMeta{
			{
				DataWeight: 1,
			},
			{
				DataWeight: 100000000000,
			},
		},
		cfg: &yt.YtSource{DesiredPartSizeBytes: 1},
		lgr: logger.Log,
	}
	res, err := dataObjs.uniformParts()
	require.NoError(t, err)
	require.Equal(t, map[int]int{0: 1, 1: 1023}, res)
}

func TestUniformParts(t *testing.T) {
	dataObjs := &YTDataObjects{
		tbls: []*tablemeta.YtTableMeta{
			{
				DataWeight: 104,
			},
			{
				DataWeight: 26889,
			},
			{
				DataWeight: 1030000,
			},
		},
		cfg: &yt.YtSource{DesiredPartSizeBytes: 1024},
		lgr: logger.Log,
	}
	res, err := dataObjs.uniformParts()
	require.NoError(t, err)
	require.Equal(t, map[int]int{0: 1, 1: 26, 2: 997}, res)
}

func TestUniformPartsWithoutDesiredSize(t *testing.T) {
	dataObjs := &YTDataObjects{
		tbls: []*tablemeta.YtTableMeta{
			{
				DataWeight: 1024,
			},
			{
				DataWeight: 2048,
			},
			{
				DataWeight: 3072,
			},
		},
		cfg: &yt.YtSource{DesiredPartSizeBytes: 1},
		lgr: logger.Log,
	}
	res, err := dataObjs.uniformParts()
	require.NoError(t, err)
	require.Equal(t, map[int]int{0: 170, 1: 341, 2: 513}, res)
}
