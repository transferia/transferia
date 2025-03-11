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
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/test/canon"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/coordinator"
	"github.com/transferria/transferria/pkg/abstract/model"
	mysql_storage "github.com/transferria/transferria/pkg/providers/mysql"
	"github.com/transferria/transferria/pkg/runtime/local"
	"github.com/transferria/transferria/tests/helpers"
)

const (
	tableName         = "__test1"
	timezoneTableName = "__test2"
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

	var rowsValuesOnSnapshot []any
	var rowsValuesOnReplication []any

	table := abstract.TableDescription{Name: tableName, Schema: source.Database}
	err = storage.LoadTable(context.Background(), table, func(input []abstract.ChangeItem) error {
		for _, item := range input {
			if item.Kind != "insert" {
				continue
			}
			rowsValuesOnSnapshot = append(rowsValuesOnSnapshot, item.ColumnValues)
		}
		return nil
	})
	require.NoError(t, err)

	var sinker mockSinker
	target := model.MockDestination{SinkerFactory: func() abstract.Sinker {
		return &sinker
	}}
	transfer := model.Transfer{
		ID:  "test",
		Src: source,
		Dst: &target,
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
			rowsValuesOnReplication = append(rowsValuesOnReplication, item.ColumnValues)
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
		INSERT INTO %s (ts) VALUES
			('2020-12-23 10:11:12'),
			('2020-12-23 14:15:16');
	`, tableName))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	require.NoError(t, <-errCh)

	require.Len(t, rowsValuesOnSnapshot, len(rowsValuesOnReplication))
	for i := range rowsValuesOnSnapshot {
		snapshotColumnValues, ok := rowsValuesOnSnapshot[i].([]any)
		require.True(t, ok)
		replicationColumnValues, ok := rowsValuesOnReplication[i].([]any)
		require.True(t, ok)
		require.Equal(t, snapshotColumnValues[1], replicationColumnValues[1])
	}

	allValues := append(rowsValuesOnSnapshot, rowsValuesOnReplication...)
	canon.SaveJSON(t, allValues)
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
