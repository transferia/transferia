package sharded_snapshot

import (
	"context"
	_ "embed"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/oracle/oraclerecipe"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/tests/helpers"
)

//go:embed dump/init.sql
var initSQL string

var (
	TransferType = abstract.TransferTypeSnapshotOnly

	Source = *oraclerecipe.RecipeOracleSource()

	dstPort, _ = strconv.Atoi(os.Getenv("PG_LOCAL_PORT"))
	Target     = provider_postgres.PgDestination{
		Hosts:    []string{"localhost"},
		User:     os.Getenv("PG_LOCAL_USER"),
		Password: model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database: os.Getenv("PG_LOCAL_DATABASE"),
		Port:     dstPort,
		Cleanup:  model.Drop,
	}
)

func init() {
	_ = os.Setenv("YC", "1")
	Source.IncludeTables = []string{"DT_SHARD.*"}
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType)
	if err := oraclerecipe.ExecSQL(context.Background(), &Source, initSQL); err != nil {
		panic(err)
	}
}

// TestShardedSnapshot verifies that a 2-worker sharded snapshot using ROWID-range
// splitting (via dba_extents) transfers all rows correctly. RowIDBytesPerShard is set
// to a tiny value so that even the small 1000-row test tables produce multiple ROWID
// ranges. The test asserts that parts were actually created with ROWID WHERE clauses
// (not ORA_HASH or unsharded), and that final row counts match.
func TestShardedSnapshot(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Oracle source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	Source.RowIDBytesPerShard = 16 * 1024 // ~2 extents per range → ~6 ranges for 1000 rows with 8KB extents

	cp := coordinator.NewStatefulFakeClient()
	transfer := helpers.WithLocalRuntime(
		helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType),
		2, // 2 workers → snapshotShardsNum=2, triggers sharding
		1,
	)

	_, err := helpers.ActivateShardedWithCP(context.Background(), cp, nil, transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	// Verify ROWID sharding was actually used: parts must contain ROWID WHERE clauses.
	parts := cp.AnyOperationTablesParts()
	require.NotEmpty(t, parts, "expected sharded parts, but none were created")
	for _, p := range parts {
		require.True(t,
			strings.Contains(p.Filter, "ROWID"),
			"part filter must use ROWID, got: %s", p.Filter)
	}

	helpers.CheckRowsCount(t, &Target, "dt_shard", "shard_pk", 1000)
	helpers.CheckRowsCount(t, &Target, "dt_shard", "shard_nopk", 1000)
}
