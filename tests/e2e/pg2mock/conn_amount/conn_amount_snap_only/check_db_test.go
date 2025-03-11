package main

import (
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	"github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/providers/postgres/pgrecipe"
	"github.com/transferria/transferria/tests/helpers"
)

const ExpectedRowCount = 1000000

type testCaseParams struct {
	testCaseName     string
	processCount     int
	user             string
	tables           []string
	checkTableLength int
}

func TestConnLimitPg2MockSnapOnly(t *testing.T) {
	// to verify that recipe exists
	_ = *pgrecipe.RecipeSource(
		pgrecipe.WithPrefix(""),
		pgrecipe.WithInitDir("init_source"),
	)

	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: helpers.GetIntFromEnv("PG_LOCAL_PORT")},
	))

	testCases := []struct {
		params testCaseParams
	}{
		{
			params: testCaseParams{
				testCaseName:     "1Thread",
				processCount:     1,
				user:             "conn_test2",
				tables:           nil,
				checkTableLength: 6,
			},
		},
		{
			params: testCaseParams{
				testCaseName:     "4Threads",
				processCount:     4,
				user:             "conn_test5",
				checkTableLength: 6,
			},
		},
		{
			params: testCaseParams{
				testCaseName:     "2Tables3Conns5Threads",
				processCount:     5,
				user:             "conn_test3",
				tables:           []string{"public.test1", "public.test2"},
				checkTableLength: 2,
			},
		},
	}

	for _, testCase := range testCases {
		func(params testCaseParams) {
			t.Run(params.testCaseName, func(t *testing.T) {
				tableRowCounts := make(map[string]int)
				source := postgres.PgSource{
					Hosts:    []string{"localhost"},
					User:     params.user,
					Password: "aA_12345",
					Database: os.Getenv("PG_LOCAL_DATABASE"),
					Port:     helpers.GetIntFromEnv("PG_LOCAL_PORT"),
					DBTables: params.tables,
				}
				source.WithDefaults()
				mutex := sync.Mutex{}
				pushCallback := func(items []abstract.ChangeItem) {
					for _, changeItem := range items {
						if changeItem.IsRowEvent() {
							mutex.Lock()
							tableRowCounts[changeItem.Table]++
							mutex.Unlock()
						}
					}
				}

				sinker := &helpers.MockSink{PushCallback: pushCallback}
				target := model.MockDestination{
					SinkerFactory: func() abstract.Sinker { return sinker },
					Cleanup:       model.DisabledCleanup,
				}
				transfer := helpers.MakeTransfer("fake", &source, &target, abstract.TransferTypeSnapshotOnly)
				transfer.Runtime = &abstract.LocalRuntime{ShardingUpload: abstract.ShardUploadParams{JobCount: 1, ProcessCount: params.processCount}}
				worker := helpers.Activate(t, transfer)
				defer worker.Close(t)
				for _, val := range tableRowCounts {
					require.Equal(t, ExpectedRowCount, val)
				}
				require.Equal(t, params.checkTableLength, len(tableRowCounts))
			})
		}(testCase.params)
	}

}
