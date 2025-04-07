package snapshot

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse"
	chrecipe "github.com/transferia/transferia/pkg/providers/clickhouse/recipe"
	"github.com/transferia/transferia/pkg/worker/tasks"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	databaseName = "db"
	tableName    = "test_table"
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = *chrecipe.MustSource(chrecipe.WithInitFile("dump/src.sql"), chrecipe.WithDatabase(databaseName))
	Target       = *chrecipe.MustTarget(chrecipe.WithInitFile("dump/dst.sql"), chrecipe.WithDatabase(databaseName), chrecipe.WithPrefix("DB0_"))
)

const cursorField = "Birthday"
const cursorValue = "2019-01-03"

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType)
	Target.Cleanup = model.DisabledCleanup
}

func TestIncrementalSnapshot(t *testing.T) {
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "CH source", Port: Source.NativePort},
		helpers.LabeledPort{Label: "CH target", Port: Target.NativePort},
	))

	transfer := helpers.MakeTransferForIncrementalSnapshot(helpers.TransferID, &Source, &Target, TransferType, databaseName, tableName, cursorField, cursorValue, 15)
	transfer.Runtime = new(abstract.LocalRuntime)

	tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
	require.NoError(t, err)

	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewStatefulFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	err = snapshotLoader.UploadTables(context.Background(), tables.ConvertToTableDescriptions(), true)
	require.NoError(t, err)

	require.NoError(t, helpers.WaitDestinationEqualRowsCount(databaseName, tableName, helpers.GetSampleableStorageByModel(t, Target), 60*time.Second, 5))

	conn, err := clickhouse.MakeConnection(Source.ToStorageParams())
	require.NoError(t, err)

	addData(t, conn)

	err = snapshotLoader.UploadTables(context.Background(), tables.ConvertToTableDescriptions(), true)
	require.NoError(t, err)

	require.NoError(t, helpers.WaitDestinationEqualRowsCount(databaseName, tableName, helpers.GetSampleableStorageByModel(t, Target), 60*time.Second, 6))

	ids := readIdsFromTarget(t, helpers.GetSampleableStorageByModel(t, Target))
	require.True(t, yslices.ContainsAll(ids, []uint16{1, 2, 3, 4, 5, 7}))
}

func addData(t *testing.T, conn *sql.DB) {
	query := fmt.Sprintf("INSERT INTO %s.%s (`Id`, `Name`, `Age`, `Birthday`) VALUES (7, 'Mary', 19, '2019-01-07');", databaseName, tableName)
	_, err := conn.Exec(query)
	require.NoError(t, err)
}

func readIdsFromTarget(t *testing.T, storage abstract.SampleableStorage) []uint16 {
	ids := make([]uint16, 0)

	require.NoError(t, storage.LoadTable(context.Background(), abstract.TableDescription{
		Name:   tableName,
		Schema: databaseName,
		Filter: "",
		EtaRow: 0,
		Offset: 0,
	}, func(items []abstract.ChangeItem) error {
		for _, row := range items {
			if !row.IsRowEvent() {
				continue
			}
			id := row.ColumnNameIndex("Id")
			ids = append(ids, row.ColumnValues[id].(uint16))
		}
		return nil
	}))
	return ids
}
