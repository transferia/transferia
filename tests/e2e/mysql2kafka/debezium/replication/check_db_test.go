package main

import (
	"context"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/library/go/test/canon"
	"github.com/transferria/transferria/library/go/test/yatest"
	"github.com/transferria/transferria/pkg/abstract"
	dp_model "github.com/transferria/transferria/pkg/abstract/model"
	kafka_provider "github.com/transferria/transferria/pkg/providers/kafka"
	"github.com/transferria/transferria/pkg/providers/mysql"
	"github.com/transferria/transferria/pkg/util"
	"github.com/transferria/transferria/tests/helpers"
)

var (
	Source = helpers.RecipeMysqlSource()
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

func eraseMeta(in string) string {
	result := in
	tsmsRegexp := regexp.MustCompile(`"ts_ms":\d+`)
	result = tsmsRegexp.ReplaceAllString(result, `"ts_ms":0`)
	return result
}

func TestReplication(t *testing.T) {
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "Mysql source", Port: Source.Port},
	))
	//------------------------------------------------------------------------------
	//initialize variables
	// fill 't' by giant random string
	insertStmt, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/tests/e2e/mysql2kafka/debezium/replication/testdata/insert.sql"))
	require.NoError(t, err)
	update1Stmt, err := os.ReadFile(yatest.SourcePath("transfer_manager/go/tests/e2e/mysql2kafka/debezium/replication/testdata/update_string.sql"))
	require.NoError(t, err)
	update2Stmt := `UPDATE customers3 SET bool1=true WHERE bool1=false;`
	// update with pkey change
	update3Stmt := `UPDATE customers3 SET pk=2 WHERE pk=1;`
	deleteStmt := `DELETE FROM customers3 WHERE 1=1;`

	//------------------------------------------------------------------------------
	//prepare dst

	dst, err := kafka_provider.DestinationRecipe()
	require.NoError(t, err)
	dst.Topic = "dbserver1"
	dst.FormatSettings = dp_model.SerializationFormat{Name: dp_model.SerializationFormatDebezium}

	// prepare additional transfer: from dst to mock

	result := make([]abstract.ChangeItem, 0)
	mockSink := &helpers.MockSink{
		PushCallback: func(in []abstract.ChangeItem) {
			abstract.Dump(in)
			for _, el := range in {
				if len(el.ColumnValues) > 0 {
					result = append(result, el)
				}
			}
		},
	}
	mockTarget := dp_model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return mockSink },
		Cleanup:       dp_model.DisabledCleanup,
	}
	additionalTransfer := helpers.MakeTransfer("additional", &kafka_provider.KafkaSource{
		Connection:  dst.Connection,
		Auth:        dst.Auth,
		GroupTopics: []string{dst.Topic},
		IsHomo:      true,
	}, &mockTarget, abstract.TransferTypeIncrementOnly)

	// activate main transfer

	helpers.InitSrcDst(helpers.TransferID, Source, dst, abstract.TransferTypeIncrementOnly)
	transfer := helpers.MakeTransfer(helpers.TransferID, Source, dst, abstract.TransferTypeIncrementOnly)

	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	go func() {
		for {
			// restart transfer if error
			errCh := make(chan error, 1)
			w, err := helpers.ActivateErr(additionalTransfer, func(err error) {
				errCh <- err
			})
			require.NoError(t, err)
			_, ok := util.Receive(ctx, errCh)
			if !ok {
				return
			}
			w.Close(t)
		}
	}()
	//-----------------------------------------------------------------------------------------------------------------
	// execute SQL statements

	connParams, err := mysql.NewConnectionParams(Source.ToStorageParams())
	require.NoError(t, err)
	srcConn, err := mysql.Connect(connParams, nil)
	require.NoError(t, err)
	defer srcConn.Close()

	_, err = srcConn.Exec(string(insertStmt))
	require.NoError(t, err)
	_, err = srcConn.Exec(string(update1Stmt))
	require.NoError(t, err)
	_, err = srcConn.Exec(update2Stmt)
	require.NoError(t, err)
	_, err = srcConn.Exec(update3Stmt)
	require.NoError(t, err)
	_, err = srcConn.Exec(deleteStmt)
	require.NoError(t, err)

	//-----------------------------------------------------------------------------------------------------------------

	for {
		if len(result) == 6 {
			canonData := make([]string, 6)
			for i := 0; i < len(result); i += 1 {
				canonVal := eraseMeta(string(kafka_provider.GetKafkaRawMessageData(&result[0])))
				canonData = append(canonData, canonVal)
			}
			canon.SaveJSON(t, canonData)
			break
		}
		time.Sleep(time.Second)
	}
}
