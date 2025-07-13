package filterrowsbyids

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	cpclient "github.com/transferia/transferia/pkg/abstract/coordinator"
	pgcommon "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/transformer/registry/filter"
	filterrowsbyids "github.com/transferia/transferia/pkg/transformer/registry/filter_rows_by_ids"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	Source = pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump"))
	Target = pgrecipe.RecipeTarget()
)

func init() {
	_ = os.Setenv("YC", "1")                                                                          // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, Source, Target, abstract.TransferTypeSnapshotAndIncrement) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	t.Run("FilterRowsByIds", func(t *testing.T) {
		t.Run("Replication", Replication)
	})
}

func runTransfer(t *testing.T, source *pgcommon.PgSource, target *pgcommon.PgDestination) *local.LocalWorker {
	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, abstract.TransferTypeSnapshotAndIncrement)

	transformer, err := filterrowsbyids.NewFilterRowsByIDsTransformer(
		filterrowsbyids.Config{
			Tables: filter.Tables{
				IncludeTables: []string{"testtable"},
			},
			Columns: filter.Columns{
				IncludeColumns: []string{"id", "id2"},
			},
			AllowedIDs: []string{
				// should match with `id` value during initial copying
				"ID1",
				// should match with `id2` value during initial copying
				"ID2_2",
				// should match with `id` value during replicating
				"ID4",
			},
		},
		logger.Log,
	)
	require.NoError(t, err)
	helpers.AddTransformer(t, transfer, transformer)

	err = tasks.ActivateDelivery(context.TODO(), nil, cpclient.NewFakeClient(), *transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	localWorker := local.NewLocalWorker(cpclient.NewFakeClient(), transfer, helpers.EmptyRegistry(), logger.Log)
	localWorker.Start()
	return localWorker
}

func Replication(t *testing.T) {
	worker := runTransfer(t, Source, Target)
	defer func(worker *local.LocalWorker) {
		_ = worker.Stop()
	}(worker)

	// update while replicating
	{
		srcConn, err := pgcommon.MakeConnPoolFromSrc(Source, logger.Log)
		require.NoError(t, err)
		defer srcConn.Close()

		_, err = srcConn.Exec(context.Background(),
			`update testtable set val = 1 where id = 'ID0'`)
		require.NoError(t, err)
		_, err = srcConn.Exec(context.Background(),
			`update testtable set val = 2 where id = 'ID1'`)
		require.NoError(t, err)
		_, err = srcConn.Exec(context.Background(),
			`update testtable set val = 3 where id = 'ID2'`)
		require.NoError(t, err)
		_, err = srcConn.Exec(context.Background(),
			`update testtable set val = 4 where id = 'ID3'`)
		require.NoError(t, err)
		_, err = srcConn.Exec(context.Background(),
			`insert into testtable (id, id2, val) values ('ID4', 'ID2_4', 4)`)
		require.NoError(t, err)
	}

	// check
	{
		require.NoError(t, helpers.WaitDestinationEqualRowsCount("public", "testtable", helpers.GetSampleableStorageByModel(t, Target), 2*time.Minute, 3))

		dstConn, err := pgcommon.MakeConnPoolFromSrc(Source, logger.Log)
		require.NoError(t, err)
		defer dstConn.Close()

		var val int

		err = dstConn.QueryRow(context.Background(), `SELECT val FROM testtable WHERE id = 'ID1'`).Scan(&val)
		require.NoError(t, err)
		require.Equal(t, 2, val)
		err = dstConn.QueryRow(context.Background(), `SELECT val FROM testtable WHERE id = 'ID2'`).Scan(&val)
		require.NoError(t, err)
		require.Equal(t, 3, val)
		err = dstConn.QueryRow(context.Background(), `SELECT val FROM testtable WHERE id = 'ID4'`).Scan(&val)
		require.NoError(t, err)
		require.Equal(t, 4, val)
	}
}
