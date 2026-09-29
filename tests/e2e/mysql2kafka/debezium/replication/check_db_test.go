package main

import (
	"context"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/library/go/test/yatest"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_kafka "github.com/transferia/transferia/pkg/providers/kafka"
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/tests/helpers/delivery"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
	"github.com/transferia/transferia/tests/helpers/mysql"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/kafka"
	_ "github.com/transferia/transferia/tests/helpers/registration/mysql"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	Source = mysql.RecipeMysqlSource()
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
	defer require.NoError(t, network.CheckConnections(
		network.LabeledPort{Label: "Mysql source", Port: Source.Port},
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

	dst, err := provider_kafka.DestinationRecipe()
	require.NoError(t, err)
	dst.Topic = "dbserver1"
	dst.FormatSettings = model.SerializationFormat{Name: model.SerializationFormatDebezium}

	// prepare additional transfer: from dst to mock

	result := make([]abstract.ChangeItem, 0)
	mockSink := mocksink.NewMockSink(func(in []abstract.ChangeItem) error {
		abstract.Dump(in)
		for _, el := range in {
			if len(el.ColumnValues) > 0 {
				result = append(result, el)
			}
		}
		return nil
	})
	mockTarget := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return mockSink },
		Cleanup:       model.DisabledCleanup,
	}
	additionalTransfer := transferhelpers.MakeTransfer("additional", &provider_kafka.KafkaSource{
		Connection:  dst.Connection,
		Auth:        dst.Auth,
		GroupTopics: []string{dst.Topic},
	}, &mockTarget, abstract.TransferTypeIncrementOnly)

	// activate main transfer

	transferhelpers.InitSrcDst(transferhelpers.TransferID, Source, dst, abstract.TransferTypeIncrementOnly)
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, Source, dst, abstract.TransferTypeIncrementOnly)

	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	go func() {
		for {
			// restart transfer if error
			errCh := make(chan error, 1)
			w, err := delivery.ActivateErr(additionalTransfer, func(err error) {
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

	connParams, err := provider_mysql.NewConnectionParams(Source.ToStorageParams())
	require.NoError(t, err)
	srcConn, err := provider_mysql.Connect(connParams, nil)
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
				vv, _ := changeitem.GetRawMessageData(result[0])
				canonVal := eraseMeta(string(vv))
				canonData = append(canonData, canonVal)
			}
			canon.SaveJSON(t, canonData)
			break
		}
		time.Sleep(time.Second)
	}
}
