package light

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"

	mysql_client "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/providers/mysql"
	"github.com/transferria/transferria/pkg/worker/tasks"
	"github.com/transferria/transferria/tests/helpers"
)

type testCaseParams struct {
	testCaseName     string
	tables           []string
	checkTableLength int
	shouldBeError    bool
	transferType     abstract.TransferType
}

var requests = []string{
	"insert into test2(name, email, age) values ('name2', 'email2', 44);",
	"insert into test(name, email, age) values ('name_test', 'email_test', 1);",
}

func getCfg(source mysql.MysqlSource) *mysql_client.Config {
	cfg := mysql_client.NewConfig()
	cfg.Addr = fmt.Sprintf("%v:%v", source.Host, source.Port)
	cfg.User = source.User
	cfg.Passwd = string(source.Password)
	cfg.DBName = source.Database
	cfg.Net = "tcp"
	return cfg
}

func TestMySQLHeteroViewsInteraction(t *testing.T) {
	testCases := []testCaseParams{
		{
			testCaseName:     "SnapOnlyViewsStored",
			checkTableLength: 4,
			transferType:     abstract.TransferTypeSnapshotOnly,
		},
		{
			testCaseName:     "SnapAndReplicaViewsNotStored",
			checkTableLength: 2,
			transferType:     abstract.TransferTypeSnapshotAndIncrement,
		},
		{
			testCaseName:     "SnapAndReplicaOnlyViewsError",
			tables:           []string{"test_view", "test_view2"},
			checkTableLength: 2,
			transferType:     abstract.TransferTypeSnapshotAndIncrement,
			shouldBeError:    true,
		},
	}

	for _, testCase := range testCases {
		func(params testCaseParams) {
			t.Run(params.testCaseName, func(t *testing.T) {
				notesCounter := make(map[string]int)
				mutex := sync.RWMutex{}
				source := *helpers.RecipeMysqlSource()
				source.IncludeTableRegex = params.tables
				defer func() {
					require.NoError(t, helpers.CheckConnections(
						helpers.LabeledPort{Label: "Mysql source", Port: source.Port},
					))
				}()
				sinker := &helpers.MockSink{PushCallback: func(items []abstract.ChangeItem) {
					for _, item := range items {
						if item.IsRowEvent() {
							mutex.Lock()
							notesCounter[item.Table]++
							mutex.Unlock()
						}
					}
				}}
				target := model.MockDestination{
					SinkerFactory: func() abstract.Sinker { return sinker },
					Cleanup:       model.DisabledCleanup,
				}
				transfer := helpers.MakeTransfer("fake", &source, &target, params.transferType)
				worker, err := helpers.ActivateErr(transfer)
				if params.shouldBeError {
					require.Error(t, err)
					require.ErrorIs(t, err, tasks.NoTablesError)
					return
				}
				require.NoError(t, err)
				defer worker.Close(t)
				var notesPerTable int
				if params.transferType == abstract.TransferTypeSnapshotAndIncrement {
					mysqlConnector, err := mysql_client.NewConnector(getCfg(source))
					require.NoError(t, err)
					db := sql.OpenDB(mysqlConnector)

					conn, err := db.Conn(context.Background())
					require.NoError(t, err)

					for _, request := range requests {
						rows, err := conn.QueryContext(context.Background(), request)
						require.NoError(t, err)
						require.NoError(t, rows.Close())
					}
					notesPerTable = 3
				}
				notesPerTable = 2
				require.Equal(t, params.checkTableLength, func() int {
					mutex.RLock()
					defer mutex.RUnlock()
					return len(notesCounter)
				}())
				for table := range notesCounter {
					require.Equal(t, notesPerTable, func(table string) int {
						mutex.RLock()
						defer mutex.RUnlock()
						return notesCounter[table]
					}(table))
				}
			})
		}(testCase)
	}

}
