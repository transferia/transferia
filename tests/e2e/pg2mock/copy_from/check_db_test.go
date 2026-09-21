package copyfrom

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/postgres"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

func TestExcludeTablesWithEmptyWhitelist(t *testing.T) {
	source := pgrecipe.RecipeSource(pgrecipe.WithPrefix(""))
	source.WithDefaults()
	sinker := mocksink.NewMockSink(nil)
	target := &model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinker },
	}
	transferhelpers.InitSrcDst(transferhelpers.TransferID, source, target, abstract.TransferTypeIncrementOnly) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
	var changes []abstract.ChangeItem
	sinker.PushCallback = func(input []abstract.ChangeItem) error {
		for _, item := range input {
			if item.Kind == abstract.InsertKind {
				fmt.Printf("changeItem dump:%s\n", item.ToJSONString())
				changes = append(changes, item)
			}
		}
		return nil
	}

	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: source.Port},
		))
	}()

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, source, target, abstract.TransferTypeSnapshotAndIncrement)
	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	connConfig, err := provider_postgres.MakeConnConfigFromSrc(logger.Log, source)
	require.NoError(t, err)
	srcConn, err := provider_postgres.NewPgConnPool(connConfig, logger.Log)
	require.NoError(t, err)

	inputRows := [][]any{
		{3, "Max"},
		{4, "Alina"},
	}
	n, err := srcConn.CopyFrom(context.Background(), pgx.Identifier{"copy_from"}, []string{"personid", "lastname"}, pgx.CopyFromRows(inputRows))
	require.NoError(t, err)
	require.Equal(t, int64(2), n)

	for {
		time.Sleep(time.Second)
		if len(changes) == 2 {
			break
		}
	}
}
