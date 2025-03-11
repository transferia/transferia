package nonutf8charset

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	mysql_storage "github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	db     = os.Getenv("RECIPE_MYSQL_SOURCE_DATABASE")
	source = helpers.WithMysqlInclude(
		helpers.RecipeMysqlSource(),
		[]string{fmt.Sprintf("%s.kek", db)},
	)
)

func init() {
	source.WithDefaults()
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

func TestNonUtf8Charset(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "Mysql source", Port: source.Port},
		))
	}()

	storage, err := mysql_storage.NewStorage(source.ToStorageParams())
	require.NoError(t, err)

	called := false
	table := abstract.TableDescription{Name: "kek", Schema: source.Database}
	err = storage.LoadTable(context.Background(), table, func(input []abstract.ChangeItem) error {
		i := 0
		for _, item := range input {
			if item.Kind != "insert" {
				continue
			}
			require.Len(t, item.ColumnValues, 2)
			if i == 0 {
				require.EqualValues(t, 1, item.ColumnValues[0])
				require.EqualValues(t, "абыр", item.ColumnValues[1])
			} else {
				require.EqualValues(t, 2, item.ColumnValues[0])
				require.EqualValues(t, "валг", item.ColumnValues[1])
			}
			i++
		}
		if i != 2 {
			return nil
		}
		require.EqualValues(t, 2, i)
		called = true
		return nil
	})
	require.NoError(t, err)
	require.True(t, called)

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

	var haveBambarbia, haveKirgudu bool
	sinker.pushCallback = func(input []abstract.ChangeItem) error {
		logger.Log.Info("Got items:")
		abstract.Dump(input)
		for _, item := range input {
			if item.Kind != "insert" {
				continue
			}
			require.Len(t, item.ColumnValues, 2)
			if item.ColumnValues[0].(int32) == 3 {
				require.EqualValues(t, item.ColumnValues[1].(string), "бамбарбия")
				haveBambarbia = true
			} else {
				require.EqualValues(t, item.ColumnValues[1].(string), "киргуду")
				haveKirgudu = true
			}
			if haveBambarbia && haveKirgudu {
				_ = wrk.Stop()
			}
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
	_, err = db.Exec("INSERT INTO kek VALUES (3, 'бамбарбия')")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO kek VALUES (4, 'киргуду')")
	require.NoError(t, err)

	require.NoError(t, <-errCh)
}
