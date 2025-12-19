package nonutf8charset

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	mysql_storage "github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/tests/helpers"
)

const (
	tableName         = "__test1"
	timezoneTableName = "__test2"
	fallbackTableName = "__test3"
)

var (
	db     = os.Getenv("RECIPE_MYSQL_SOURCE_DATABASE")
	source = helpers.WithMysqlInclude(
		helpers.RecipeMysqlSource(),
		[]string{fmt.Sprintf("%s.%s", db, tableName)},
	)
)

func init() {
	source.WithDefaults()
	source.Timezone = "Europe/Moscow"
}

type mockSinker struct {
	pushCallback func(input []abstract.ChangeItem) error
}

func (s *mockSinker) Push(input []abstract.ChangeItem) error {
	return s.pushCallback(input)
}

func (s *mockSinker) Close() error {
	return nil
}

func makeConnConfig() *mysql.Config {
	cfg := mysql.NewConfig()
	cfg.Addr = fmt.Sprintf("%v:%v", source.Host, source.Port)
	cfg.User = source.User
	cfg.Passwd = string(source.Password)
	cfg.DBName = source.Database
	cfg.Net = "tcp"
	return cfg
}

func TestTimeZoneSnapshotAndReplication(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: source.Port},
		))
	}()

	storage, err := mysql_storage.NewStorage(source.ToStorageParams())
	require.NoError(t, err)

	var rowsValuesOnSnapshot []map[string]any
	var rowsValuesOnReplication []map[string]any

	table := abstract.TableDescription{Name: tableName, Schema: source.Database}
	err = storage.LoadTable(context.Background(), table, func(input []abstract.ChangeItem) error {
		for _, item := range input {
			if item.Kind != "insert" {
				continue
			}

			row := make(map[string]any)
			for idx, colName := range item.ColumnNames {
				row[colName] = item.ColumnValues[idx]
			}
			rowsValuesOnSnapshot = append(rowsValuesOnSnapshot, row)
		}
		return nil
	})
	require.NoError(t, err)

	var sinker mockSinker
	target := model.MockDestination{SinkerFactory: func() abstract.Sinker {
		return &sinker
	}}
	transfer := model.Transfer{
		ID:                "test",
		Src:               source,
		Dst:               &target,
		TypeSystemVersion: 11,
	}

	fakeClient := coordinator.NewStatefulFakeClient()
	err = mysql_storage.SyncBinlogPosition(source, transfer.ID, fakeClient)
	require.NoError(t, err)

	wrk := local.NewLocalWorker(fakeClient, &transfer, helpers.EmptyRegistry(), logger.Log)

	sinker.pushCallback = func(input []abstract.ChangeItem) error {
		for _, item := range input {
			if item.Kind != "insert" {
				continue
			}

			row := make(map[string]any)
			for idx, colName := range item.ColumnNames {
				row[colName] = item.ColumnValues[idx]
			}
			rowsValuesOnReplication = append(rowsValuesOnReplication, row)
		}

		if len(rowsValuesOnSnapshot)+len(rowsValuesOnReplication) >= 4 {
			_ = wrk.Stop()
		}

		return nil
	}

	errCh := make(chan error)
	go func() {
		errCh <- wrk.Run()
	}()

	conn, err := mysql.NewConnector(makeConnConfig())
	require.NoError(t, err)
	db := sql.OpenDB(conn)

	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	require.NoError(t, err)

	_, err = tx.Query("SET SESSION time_zone = '+00:00';")
	require.NoError(t, err)

	_, err = tx.Query(fmt.Sprintf(`
		INSERT INTO %s (ts, dt) VALUES
			('2020-12-23 10:11:12', '2020-12-23 10:11:12'),
			('2020-12-23 14:15:16', '2020-12-23 14:15:16');
	`, tableName))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	require.NoError(t, <-errCh)

	colNamesForCheck := []string{"dt", "ts"}
	require.Len(t, rowsValuesOnSnapshot, len(rowsValuesOnReplication))
	for idx := range rowsValuesOnSnapshot {
		for _, colName := range colNamesForCheck {
			require.Equal(t, rowsValuesOnSnapshot[idx][colName], rowsValuesOnReplication[idx][colName])
		}
	}

	dataForCanon := map[string][]map[string]any{
		"snapshot":    rowsValuesOnSnapshot,
		"replication": rowsValuesOnReplication,
	}
	canon.SaveJSON(t, dataForCanon)
}

func TestDifferentTimezones(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: source.Port},
		))
	}()

	storageCfg := source.ToStorageParams()
	checkTimezoneVals := func(cfg *mysql_storage.MysqlStorageParams, timezone string, expectedRows []any) {
		cfg.Timezone = timezone
		storage, err := mysql_storage.NewStorage(cfg)
		require.NoError(t, err)
		defer storage.Close()

		var rows []any
		table := abstract.TableDescription{Name: timezoneTableName, Schema: source.Database}
		err = storage.LoadTable(context.Background(), table, func(input []abstract.ChangeItem) error {
			for _, item := range input {
				if item.Kind != "insert" {
					continue
				}
				rows = append(rows, item.ColumnValues)
			}
			return nil
		})
		require.NoError(t, err)

		require.Equal(t, expectedRows, rows)
	}

	timezone := ""
	loc, err := time.LoadLocation(timezone)
	require.NoError(t, err)
	t1, _ := time.ParseInLocation(time.DateTime, "2020-12-31 10:00:00", loc)
	t2, _ := time.ParseInLocation(time.DateTime, "2020-12-31 14:00:00", loc)
	checkTimezoneVals(storageCfg, timezone, []any{
		[]any{int32(1), t1},
		[]any{int32(2), t2},
	})

	timezone = "UTC"
	loc, err = time.LoadLocation(timezone)
	require.NoError(t, err)
	t1, _ = time.ParseInLocation(time.DateTime, "2020-12-31 10:00:00", loc)
	t2, _ = time.ParseInLocation(time.DateTime, "2020-12-31 14:00:00", loc)
	checkTimezoneVals(storageCfg, timezone, []any{
		[]any{int32(1), t1},
		[]any{int32(2), t2},
	})

	timezone = "Europe/Moscow"
	loc, err = time.LoadLocation(timezone)
	require.NoError(t, err)
	t1, _ = time.ParseInLocation(time.DateTime, "2020-12-31 13:00:00", loc)
	t2, _ = time.ParseInLocation(time.DateTime, "2020-12-31 17:00:00", loc)
	checkTimezoneVals(storageCfg, timezone, []any{
		[]any{int32(1), t1},
		[]any{int32(2), t2},
	})

	timezone = "America/Buenos_Aires"
	loc, err = time.LoadLocation(timezone)
	require.NoError(t, err)
	t1, _ = time.ParseInLocation(time.DateTime, "2020-12-31 07:00:00", loc)
	t2, _ = time.ParseInLocation(time.DateTime, "2020-12-31 11:00:00", loc)
	checkTimezoneVals(storageCfg, timezone, []any{
		[]any{int32(1), t1},
		[]any{int32(2), t2},
	})
}

func TestDatetimeTimeZoneFallback(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: source.Port},
		))
	}()

	currentSrcCfg := *source
	currentSrcCfg.Timezone = "Europe/Moscow"
	currentSrcCfg.IncludeTableRegex = []string{fallbackTableName}

	var sinker mockSinker
	target := model.MockDestination{
		SinkerFactory: func() abstract.Sinker {
			return &sinker
		},
		Cleanup: model.DisabledCleanup,
	}
	transfer := &model.Transfer{
		ID:   "test",
		Src:  &currentSrcCfg,
		Dst:  &target,
		Type: abstract.TransferTypeSnapshotOnly,
	}

	makePushCallback := func(result *[]abstract.ChangeItem) func(input []abstract.ChangeItem) error {
		return func(input []abstract.ChangeItem) error {
			for _, item := range input {
				if item.IsRowEvent() {
					*result = append(*result, item)
				}
			}
			return nil
		}
	}

	// check for type system version 10
	transfer.TypeSystemVersion = 10
	insertedRowsVersion10 := make([]abstract.ChangeItem, 0)
	sinker.pushCallback = makePushCallback(&insertedRowsVersion10)

	helpers.Activate(t, transfer, func(err error) {
		require.NoError(t, err)
	})

	// check for type system version 11
	transfer.TypeSystemVersion = 11
	insertedRowsVersion11 := make([]abstract.ChangeItem, 0)
	sinker.pushCallback = makePushCallback(&insertedRowsVersion11)

	helpers.Activate(t, transfer, func(err error) {
		require.NoError(t, err)
	})

	// compare results
	require.Equal(t, len(insertedRowsVersion10), len(insertedRowsVersion11))
	for i := range insertedRowsVersion10 {
		require.Equal(t, len(insertedRowsVersion10[i].ColumnNames), len(insertedRowsVersion11[i].ColumnNames))

		version10TsColIndex := insertedRowsVersion10[i].ColumnNameIndex("ts")
		version11TsColIndex := insertedRowsVersion11[i].ColumnNameIndex("ts")
		require.Equal(t, insertedRowsVersion10[i].ColumnValues[version10TsColIndex], insertedRowsVersion11[i].ColumnValues[version11TsColIndex])
	}

	timezone := "Europe/Moscow"
	loc, err := time.LoadLocation(timezone)
	require.NoError(t, err)
	t1Version10, _ := time.ParseInLocation(time.DateTime, "2020-12-31 10:00:00", time.UTC)
	t2Version10, _ := time.ParseInLocation(time.DateTime, "2020-12-31 14:00:00", time.UTC)
	t1Version11, _ := time.ParseInLocation(time.DateTime, "2020-12-31 10:00:00", loc)
	t2Version11, _ := time.ParseInLocation(time.DateTime, "2020-12-31 14:00:00", loc)

	dtColIndex := insertedRowsVersion10[0].ColumnNameIndex("dt")
	require.Equal(t, t1Version10, insertedRowsVersion10[0].ColumnValues[dtColIndex])
	require.Equal(t, t2Version10, insertedRowsVersion10[1].ColumnValues[dtColIndex])
	require.Equal(t, t1Version11, insertedRowsVersion11[0].ColumnValues[dtColIndex])
	require.Equal(t, t2Version11, insertedRowsVersion11[1].ColumnValues[dtColIndex])
}
