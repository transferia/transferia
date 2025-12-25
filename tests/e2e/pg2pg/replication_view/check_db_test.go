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
	pgcommon "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
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
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	transferID := helpers.TransferID
	helpers.InitSrcDst(transferID, &Source, &Target, transferType)
	transfer := helpers.MakeTransfer(transferID, &Source, &Target, transferType)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	// insert

	srcConn, err := pgcommon.MakeConnPoolFromSrc(&Source, logger.Log)
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

	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "tv_table", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 20*time.Second))
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "odd_channels", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 20*time.Second))
}
