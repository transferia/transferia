package sharded_snapshot

import (
	"context"
	_ "embed"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/oracle/oraclerecipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/clickhouse"
	_ "github.com/transferia/transferia/tests/helpers/registration/oracle"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	"github.com/transferia/transferia/tests/helpers/testmetrics"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

//go:embed dump/init.sql
var initSQL string

var (
	TransferType = abstract.TransferTypeSnapshotOnly

	Source = *oraclerecipe.RecipeOracleSource()

	Target = clickhouse_model.ChDestination{
		ShardsList: []clickhouse_model.ClickHouseShard{
			{
				Name:  "_",
				Hosts: []string{"localhost"},
			},
		},
		User:                "default",
		Password:            "",
		Database:            "target",
		HTTPPort:            testenv.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort:          testenv.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
		ProtocolUnspecified: true,
		Cleanup:             model.Drop,
	}
)

func init() {
	_ = os.Setenv("YC", "1")
	Source.IncludeTables = []string{"DT_SHARD.*"}
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, TransferType)
	if err := oraclerecipe.ExecSQL(context.Background(), &Source, initSQL); err != nil {
		panic(err)
	}
}

// TestShardedSnapshot verifies that a 2-worker sharded snapshot using ROWID-range
// splitting (via dba_extents) transfers all rows to ClickHouse. RowIDBytesPerShard is set
// to a tiny value so that even the small 1000-row test tables produce multiple ROWID
// ranges. The test asserts that parts were actually created with ROWID WHERE clauses
// (not ORA_HASH or unsharded), and that final row counts match.
func TestShardedSnapshot(t *testing.T) {
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "Oracle source", Port: Source.Port},
			network.LabeledPort{Label: "ClickHouse target", Port: Target.NativePort},
		))
	}()

	Source.RowIDBytesPerShard = 16 * 1024 // ~2 extents per range → ~6 ranges for 1000 rows with 8KB extents

	cp := coordinator.NewStatefulFakeClient()
	transfer := transferhelpers.WithLocalRuntime(
		transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, TransferType),
		2, // 2 workers → snapshotShardsNum=2, triggers sharding
		1,
	)

	_, err := delivery.ActivateShardedWithCP(context.Background(), cp, nil, transfer, testmetrics.EmptyRegistry())
	require.NoError(t, err)

	parts := cp.AnyOperationTablesParts()
	require.NotEmpty(t, parts, "expected sharded parts, but none were created")
	for _, p := range parts {
		require.True(t,
			strings.Contains(p.Filter, "ROWID"),
			"part filter must use ROWID, got: %s", p.Filter)
	}

	storagecomparison.CheckRowsCount(t, &Target, Target.Database, "shard_pk", 1000)
	storagecomparison.CheckRowsCount(t, &Target, Target.Database, "shard_nopk", 1000)
}
