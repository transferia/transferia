package replicationview

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

func TestViewReplication(t *testing.T) {
	Source := *pgrecipe.RecipeSource(pgrecipe.WithInitDir("init_source"))
	Target := *pgrecipe.RecipeTarget(pgrecipe.WithInitDir("init_target"))
	Target.Cleanup = model.Truncate
	transferType := abstract.TransferTypeIncrementOnly

	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: Source.Port},
			network.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	transferID := transferhelpers.TransferID
	transferhelpers.InitSrcDst(transferID, &Source, &Target, transferType)
	transfer := transferhelpers.MakeTransfer(transferID, &Source, &Target, transferType)

	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	// insert

	srcConn, err := provider_postgres.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()
	commands := []string{
		`INSERT INTO tv_table(i, cname) VALUES (1, 'ZDF');`,
		`INSERT INTO tv_table(i, cname) VALUES (2, 'Das Erste');`,
		`INSERT INTO tv_table(i, cname) VALUES (3, 'RTL');`,
		`INSERT INTO tv_table(i, cname) VALUES (4, 'SAT.1');`,
		`INSERT INTO tv_table(i, cname) VALUES (5, 'VOX');`,
	}
	for _, command := range commands {
		_, err = srcConn.Exec(context.Background(), command)
		require.NoError(t, err)
	}

	// check

	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "tv_table", storagecomparison.GetSampleableStorageByModel(t, Source), storagecomparison.GetSampleableStorageByModel(t, Target), 20*time.Second))
	require.NoError(t, storage.WaitEqualRowsCount(t, "public", "odd_channels", storagecomparison.GetSampleableStorageByModel(t, Source), storagecomparison.GetSampleableStorageByModel(t, Target), 20*time.Second))
}
