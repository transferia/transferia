//go:build !disable_yt_provider

package lfstaging

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/providers/yt/recipe"
)

func TestRounding(t *testing.T) {
	require.Equal(
		t,
		int64(0),
		roundTimestampToNearest(time.Unix(299, 0), time.Minute*5).Unix(),
	)
	require.Equal(
		t,
		int64(300),
		roundTimestampToNearest(time.Unix(300, 0), time.Minute*5).Unix(),
	)
	require.Equal(
		t,
		int64(0),
		roundTimestampToNearest(time.Unix(20, 0), time.Minute*5).Unix(),
	)
}

func TestStagingWriterNew(t *testing.T) {
	env, cancel := recipe.NewEnv(t)
	defer cancel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()

	tx, err := env.YT.BeginTx(ctx, nil)
	require.NoError(t, err, "Cannot start tx")
	defer tx.Abort()

	config := defaultSinkConfig()

	now, err := time.Parse(time.RFC3339, "2022-01-01T01:01:01Z")
	require.NoError(t, err, "Cannot parse time")

	_, err = newStagingWriter(tx, config, now)
	require.NoError(t, err, "newStagingWriter throws")

	tablePath := config.stagingPath.Child(config.topic).Child(makeStagingTableName(config.aggregationPeriod, now))
	exists, err := tx.NodeExists(ctx, tablePath, nil)

	require.NoError(t, err, "Cannot check output table for existence")

	require.True(t, exists)
}
