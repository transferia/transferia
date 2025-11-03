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
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/debezium"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
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

func getMessage(t *testing.T, changeItem *abstract.ChangeItem, additionalParamKey, additionalParamVal string) (string, error) {
	params := map[string]string{
		debeziumparameters.DatabaseDBName:   "database",
		debeziumparameters.TopicPrefix:      "databaseServerName",
		debeziumparameters.AddOriginalTypes: "false",
		debeziumparameters.SourceType:       "pg",
	}
	if additionalParamKey != "" {
		params[additionalParamKey] = additionalParamVal
	}

	generator, err := debezium.NewMessagesEmitter(params, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	resultKV, err := generator.EmitKV(changeItem, debezium.GetPayloadTSMS(changeItem), true, nil)
	if err != nil {
		return "", err
	}
	require.Equal(t, 1, len(resultKV))
	return *resultKV[0].DebeziumVal, nil
}

func TestSnapshotAndReplication(t *testing.T) {
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: Source.Port},
	))

	// extract changeItems

	sinker := &helpers.MockSink{}
	target := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinker },
		Cleanup:       model.DisabledCleanup,
	}
	transfer := helpers.MakeTransfer("fake", &Source, &target, abstract.TransferTypeSnapshotAndIncrement)

	myMap := make(map[string][]abstract.ChangeItem)
	index := 0
	myMutex := sync.Mutex{}
	sinker.PushCallback = func(input []abstract.ChangeItem) error {
		myMutex.Lock()
		defer myMutex.Unlock()
		for _, el := range input {
			if el.Kind != abstract.InsertKind {
				continue
			}
			fmt.Printf("changeItem:%s\n", el.ToJSONString())
			myMap[el.Table] = append(myMap[el.Table], el)
			index++
		}
		return nil
	}

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	srcConn, err := pgcommon.MakeConnPoolFromSrc(&Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	_, err = srcConn.Exec(context.Background(), `INSERT INTO history.events (park_id, profile_id, event_list) VALUES ('park4', 'profile4', '{"(\"2023-02-02 11:43:32.335573+03\",online,{driving})"}');`)
	require.NoError(t, err)
	_, err = srcConn.Exec(context.Background(), `INSERT INTO table_with_enum (id, val) VALUES (2, 'bar');`)
	require.NoError(t, err)

	err = helpers.WaitCond(15*time.Second, func() bool {
		myMutex.Lock()
		defer myMutex.Unlock()
		return len(myMap["events"])+len(myMap["table_with_enum"]) == 4
	})
	require.NoError(t, err)

	for k := range myMap {
		for i := range myMap[k] {
			myMap[k][i].ID = 0
			myMap[k][i].CommitTime = 0
			myMap[k][i].LSN = 0
		}
	}

	// run tests on dt.unknown.types.policy

	t.Run("default", func(t *testing.T) {
		_, err := getMessage(t, &myMap["events"][0], "", "")
		require.Error(t, err)
		_, err = getMessage(t, &myMap["events"][1], "", "")
		require.Error(t, err)
	})
	t.Run("fail", func(t *testing.T) {
		_, err := getMessage(t, &myMap["events"][0], debeziumparameters.UnknownTypesPolicy, "fail")
		require.Error(t, err)
		_, err = getMessage(t, &myMap["events"][1], debeziumparameters.UnknownTypesPolicy, "fail")
		require.Error(t, err)
	})
	var canonData []interface{}
	t.Run("skip", func(t *testing.T) {
		msgS, err := getMessage(t, &myMap["events"][0], debeziumparameters.UnknownTypesPolicy, "skip")
		require.NoError(t, err)
		canonData = append(canonData, msgS)
		msgR, err := getMessage(t, &myMap["events"][1], debeziumparameters.UnknownTypesPolicy, "skip")
		require.NoError(t, err)
		canonData = append(canonData, msgR)
	})
	t.Run("to_string", func(t *testing.T) {
		msgS, err := getMessage(t, &myMap["events"][0], debeziumparameters.UnknownTypesPolicy, "to_string")
		require.NoError(t, err)
		canonData = append(canonData, msgS)
		msgR, err := getMessage(t, &myMap["events"][1], debeziumparameters.UnknownTypesPolicy, "to_string")
		require.NoError(t, err)
		canonData = append(canonData, msgR)
	})

	// run tests on 'enum'

	t.Run("pg:enum", func(t *testing.T) {
		msgS, err := getMessage(t, &myMap["table_with_enum"][0], "", "")
		require.NoError(t, err)
		canonData = append(canonData, msgS)
		msgR, err := getMessage(t, &myMap["table_with_enum"][1], "", "")
		require.NoError(t, err)
		canonData = append(canonData, msgR)
	})

	// canonize

	canon.SaveJSON(t, canonData)
}
