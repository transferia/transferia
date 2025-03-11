package copyfrom

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	pgcommon "github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/providers/postgres/pgrecipe"
	"github.com/transferria/transferria/tests/helpers"
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

func TestExcludeTablesWithEmptyWhitelist(t *testing.T) {
	source := pgrecipe.RecipeSource(pgrecipe.WithPrefix(""))
	source.WithDefaults()
	sinker := &helpers.MockSink{}
	target := &model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinker },
	}
	helpers.InitSrcDst(helpers.TransferID, source, target, abstract.TransferTypeIncrementOnly) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
	var changes []abstract.ChangeItem
	sinker.PushCallback = func(input []abstract.ChangeItem) {
		for _, item := range input {
			if item.Kind == abstract.InsertKind {
				fmt.Printf("changeItem dump:%s\n", item.ToJSONString())
				changes = append(changes, item)
			}
		}
	}

	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: source.Port},
		))
	}()

	transfer := helpers.MakeTransfer(helpers.TransferID, source, target, abstract.TransferTypeSnapshotAndIncrement)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	connConfig, err := pgcommon.MakeConnConfigFromSrc(logger.Log, source)
	require.NoError(t, err)
	srcConn, err := pgcommon.NewPgConnPool(connConfig, logger.Log)
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
