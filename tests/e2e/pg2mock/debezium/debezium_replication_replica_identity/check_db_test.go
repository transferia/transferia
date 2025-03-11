package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/test/yatest"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	debeziumcommon "github.com/transferia/transferia/pkg/debezium/common"
	"github.com/transferia/transferia/pkg/debezium/testutil"
	pgcommon "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	Source = *pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("init_source"))
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

func ReadTextFiles(paths []string, out []*string) error {
	for index, path := range paths {
		valArr, err := os.ReadFile(yatest.SourcePath(path))
		if err != nil {
			return xerrors.Errorf("unable to read file %s: %w", path, err)
		}
		val := string(valArr)
		*out[index] = val
	}
	return nil
}

//---------------------------------------------------------------------------------------------------------------------

func TestReplication(t *testing.T) {
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: Source.Port},
	))

	//------------------------------------------------------------------------------
	// read files

	var canonizedDebeziumUpdateKey = ``
	var canonizedDebeziumUpdateVal = ``
	var canonizedDebeziumDeleteKey = ``
	var canonizedDebeziumDeleteVal = ``

	err := ReadTextFiles(
		[]string{
			"transfer_manager/go/tests/e2e/pg2mock/debezium/debezium_replication_replica_identity/testdata/debezium_msg_update_key.txt",
			"transfer_manager/go/tests/e2e/pg2mock/debezium/debezium_replication_replica_identity/testdata/debezium_msg_update_val.txt",
			"transfer_manager/go/tests/e2e/pg2mock/debezium/debezium_replication_replica_identity/testdata/debezium_msg_delete_key.txt",
			"transfer_manager/go/tests/e2e/pg2mock/debezium/debezium_replication_replica_identity/testdata/debezium_msg_delete_val.txt",
		},
		[]*string{
			&canonizedDebeziumUpdateKey,
			&canonizedDebeziumUpdateVal,
			&canonizedDebeziumDeleteKey,
			&canonizedDebeziumDeleteVal,
		},
	)
	require.NoError(t, err)

	fmt.Printf("canonizedDebeziumUpdateKey=%s\n", canonizedDebeziumUpdateKey)
	fmt.Printf("canonizedDebeziumUpdateVal=%s\n", canonizedDebeziumUpdateVal)
	fmt.Printf("canonizedDebeziumUpdateKey=%s\n", canonizedDebeziumDeleteKey)
	fmt.Printf("canonizedDebeziumUpdateVal=%s\n", canonizedDebeziumDeleteVal)

	//------------------------------------------------------------------------------
	// start replication

	sinker := &helpers.MockSink{}
	target := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinker },
		Cleanup:       model.DisabledCleanup,
	}
	transfer := helpers.MakeTransfer("fake", &Source, &target, abstract.TransferTypeSnapshotAndIncrement)

	mutex := sync.Mutex{}
	var changeItems []abstract.ChangeItem
	sinker.PushCallback = func(input []abstract.ChangeItem) {
		found := false
		for _, el := range input {
			if el.Table == "basic_types" {
				found = true
			}
		}
		if !found {
			return
		}
		//---
		mutex.Lock()
		defer mutex.Unlock()

		for _, el := range input {
			if el.Table != "basic_types" {
				continue
			}
			changeItems = append(changeItems, el)
		}
	}

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	//-----------------------------------------------------------------------------------------------------------------
	// execute SQL statements

	srcConn, err := pgcommon.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	_, err = srcConn.Exec(context.Background(), `UPDATE public.basic_types SET val='ururu' WHERE id=1;`)
	require.NoError(t, err)
	_, err = srcConn.Exec(context.Background(), `DELETE FROM public.basic_types WHERE id=1;`)
	require.NoError(t, err)

	for {
		time.Sleep(time.Second)

		mutex.Lock()
		if len(changeItems) == 7 {
			break
		}
		mutex.Unlock()
	}

	require.Equal(t, changeItems[0].Kind, abstract.InitShardedTableLoad)
	require.Equal(t, changeItems[1].Kind, abstract.InitTableLoad)
	require.Equal(t, changeItems[2].Kind, abstract.InsertKind)
	require.Equal(t, changeItems[3].Kind, abstract.DoneTableLoad)
	require.Equal(t, changeItems[4].Kind, abstract.DoneShardedTableLoad)
	require.Equal(t, changeItems[5].Kind, abstract.UpdateKind)
	require.Equal(t, changeItems[6].Kind, abstract.DeleteKind)

	for i := range changeItems {
		fmt.Printf("changeItem dump: %s\n", changeItems[i].ToJSONString())
	}

	//-----------------------------------------------------------------------------------------------------------------

	testSuite := []debeziumcommon.ChangeItemCanon{
		{
			ChangeItem: &changeItems[5],
			DebeziumEvents: []debeziumcommon.KeyValue{{
				DebeziumKey: canonizedDebeziumUpdateKey,
				DebeziumVal: &canonizedDebeziumUpdateVal,
			}},
		},
		{
			ChangeItem: &changeItems[6],
			DebeziumEvents: []debeziumcommon.KeyValue{
				{
					DebeziumKey: canonizedDebeziumDeleteKey,
					DebeziumVal: &canonizedDebeziumDeleteVal,
				},
				{
					DebeziumKey: canonizedDebeziumDeleteKey,
					DebeziumVal: nil,
				},
			},
		},
	}

	testSuite = testutil.FixTestSuite(t, testSuite, "fullfillment", "pguser", "pg")

	for _, testCase := range testSuite {
		testutil.CheckCanonizedDebeziumEvent(t, testCase.ChangeItem, "fullfillment", "pguser", "pg", false, testCase.DebeziumEvents)
	}
}
