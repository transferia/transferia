package replication

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/connection"
	pg_provider "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var (
	TransferType  = abstract.TransferTypeSnapshotAndIncrement
	Source        = *pgrecipe.RecipeSource(pgrecipe.WithInitDir("dump"), pgrecipe.WithPrefix(""), pgrecipe.WithConnection("connID"))
	SrcConnection = pgrecipe.ManagedConnection(pgrecipe.WithInitDir("dump"), pgrecipe.WithPrefix(""))
	Target        = *pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))
)

func init() {
	_ = os.Setenv("YC", "1")                                               // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, transferID
	helpers.InitConnectionResolver(map[string]connection.ManagedConnection{"connID": SrcConnection})
}

func TestGroup(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: SrcConnection.Hosts[0].Port},
			helpers.LabeledPort{Label: "PG target", Port: Target.Port},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Existence", Existence)
		t.Run("Verify", Verify)
		t.Run("Load", Load)
	})
}

func Existence(t *testing.T) {
	_, err := pg_provider.NewStorage(Source.ToStorageParams(nil))
	require.NoError(t, err)
	_, err = pg_provider.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)
}

func Verify(t *testing.T) {
	var transfer model.Transfer
	transfer.Src = &Source
	transfer.Dst = &Target
	transfer.Type = "INCREMENT_ONLY"

	err := tasks.VerifyDelivery(transfer, logger.Log, helpers.EmptyRegistry())
	require.NoError(t, err)

	dstStorage, err := pg_provider.NewStorage(Target.ToStorageParams())
	require.NoError(t, err)

	var result bool
	err = dstStorage.Conn.QueryRow(context.Background(), `
		SELECT EXISTS
        (
            SELECT 1
            FROM pg_tables
            WHERE schemaname = 'public'
            AND tablename = '_ping'
        );
	`).Scan(&result)
	require.NoError(t, err)
	require.Equal(t, false, result)
}

func Load(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "__test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 240*time.Second))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))

	//-----------------------------------------------------------------------------------------------------------------

	sink, err := pg_provider.NewSink(logger.Log, helpers.TransferID, Source.ToSinkParams(), helpers.EmptyRegistry())
	require.NoError(t, err)

	arrColSchema := abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "aid", DataType: ytschema.TypeUint8.String(), PrimaryKey: true},
		{ColumnName: "str", DataType: ytschema.TypeString.String(), PrimaryKey: true},
		{ColumnName: "id", DataType: ytschema.TypeUint8.String(), PrimaryKey: true},
		{ColumnName: "jb", DataType: ytschema.TypeAny.String(), PrimaryKey: false},
	})
	changeItemBuilder := helpers.NewChangeItemsBuilder("public", "__test", arrColSchema)

	require.NoError(t, sink.Push(changeItemBuilder.Inserts(t, []map[string]interface{}{{"aid": 11, "str": "a", "id": 11, "jb": "{}"}, {"aid": 22, "str": "b", "id": 22, "jb": `{"x": 1, "y": -2}`}, {"aid": 33, "str": "c", "id": 33}})))
	require.NoError(t, sink.Push(changeItemBuilder.Updates(t, []map[string]interface{}{{"aid": 33, "str": "c", "id": 34, "jb": `{"test": "test"}`}}, []map[string]interface{}{{"aid": 33, "str": "c", "id": 33}})))
	require.NoError(t, sink.Push(changeItemBuilder.Deletes(t, []map[string]interface{}{{"aid": 22, "str": "b", "id": 22}})))
	require.NoError(t, sink.Push(changeItemBuilder.Deletes(t, []map[string]interface{}{{"aid": 33, "str": "c", "id": 34}})))

	//-----------------------------------------------------------------------------------------------------------------

	helpers.CheckRowsCount(t, Source, "public", "__test", 14)
	require.NoError(t, helpers.WaitEqualRowsCount(t, "public", "__test", helpers.GetSampleableStorageByModel(t, Source), helpers.GetSampleableStorageByModel(t, Target), 240*time.Second))
	require.NoError(t, helpers.CompareStorages(t, Source, Target, helpers.NewCompareStorageParams()))
}
