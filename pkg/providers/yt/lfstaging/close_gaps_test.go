//go:build !disable_yt_provider

package lfstaging

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/providers/yt/recipe"
)

func TestClosingGaps(t *testing.T) {
	env, cancel := recipe.NewEnv(t)
	defer cancel()

	ctx := context.Background()
	config := defaultSinkConfig()

	latest, err := time.Parse(time.RFC3339, "2022-01-01T00:00:00Z")
	require.NoError(t, err, "Cannot parse time")
	now, err := time.Parse(time.RFC3339, "2022-01-01T00:01:00Z")
	require.NoError(t, err, "Cannot parse time")

	err = storeYtState(env.YT, config.tmpPath, ytState{
		LastTableTS: latest.Unix(),
	})
	require.NoError(t, err, "Cannot store initial state")

	tx, err := env.YT.BeginTx(ctx, nil)
	require.NoError(t, err, "Cannot start tx")
	defer tx.Abort()

	err = closeGaps(tx, config, now)
	require.NoError(t, err, "closeGaps throws")

	nodes, err := listNodes(tx, config.stagingPath.Child(config.topic))
	require.NoError(t, err, "Cannot list staging nodes")

	err = tx.Commit()
	require.NoError(t, err, "Cannot commit tx")

	names := []string{}
	for _, node := range nodes {
		names = append(names, node.Name)
	}

	require.ElementsMatch(
		t,
		names,
		[]string{
			"1640995220-300",
			"1640995230-300",
			"1640995210-300",
			"1640995250-300",
			"1640995240-300",
		},
	)

	var logbrokerMetadata map[string]interface{}
	err = env.YT.GetNode(
		ctx,
		config.stagingPath.Child(config.topic).Child(nodes[0].Name).Child("@_logbroker_metadata"),
		&logbrokerMetadata,
		nil,
	)
	require.NoError(t, err, "Cannot request node meta (@_logbroker_metadata)")

	// panic is ok here, it will fail the test with is exactly what is expected
	cluster := logbrokerMetadata["topics"].([]interface{})[0].(map[string]interface{})["cluster"].(string)

	require.Equal(t, cluster, "fakecluster")

	var account string
	err = env.YT.GetNode(
		ctx,
		config.stagingPath.Child(config.topic).Child(nodes[0].Name).Child("@account"),
		&account,
		nil,
	)
	require.NoError(t, err, "Cannot request node meta (@account)")
	require.Equal(t, account, "default")
}
