package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	pgcommon "github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/pkg/providers/postgres/pgrecipe"
	"github.com/transferria/transferria/tests/helpers"
)

var (
	Source = *pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("init_source"))
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

//---------------------------------------------------------------------------------------------------------------------

func TestReplication(t *testing.T) {
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: Source.Port},
	))

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
			if el.Table == "testtable2" {
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
			if el.Table != "testtable2" || el.Kind != abstract.InsertKind {
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

	_, err = srcConn.Exec(context.Background(), `INSERT INTO testtable2 VALUES (3, '{"k": 345}', '{"k": 345}')`)
	require.NoError(t, err)
	_, err = srcConn.Exec(context.Background(), `INSERT INTO testtable2 VALUES (4, '{"k": 456.7}', '{"k": 456.7}')`)
	require.NoError(t, err)

	for {
		time.Sleep(time.Second)

		mutex.Lock()
		if len(changeItems) == 4 {
			break
		}
		mutex.Unlock()
	}

	//-----------------------------------------------------------------------------------------------------------------
	// check every value is json.Number

	for _, item := range changeItems {
		changeItemMap := item.AsMap()
		val0 := changeItemMap["val_json"].(map[string]interface{})
		for _, v := range val0 {
			require.EqualValues(t, "json.Number", fmt.Sprintf("%T", v))
		}
		val1 := changeItemMap["val_jsonb"].(map[string]interface{})
		for _, v := range val1 {
			require.EqualValues(t, "json.Number", fmt.Sprintf("%T", v))
		}
	}
}
