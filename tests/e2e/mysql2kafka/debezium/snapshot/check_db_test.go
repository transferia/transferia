package main

import (
	"context"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_kafka "github.com/transferia/transferia/pkg/providers/kafka"
	"github.com/transferia/transferia/pkg/util"
	canon2 "github.com/transferia/transferia/tests/helpers/canon"
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

func TestSnapshot(t *testing.T) {
	defer require.NoError(t, network.CheckConnections(
		network.LabeledPort{Label: "Mysql source", Port: Source.Port},
	))
	//------------------------------------------------------------------------------
	//prepare dst

	dst, err := provider_kafka.DestinationRecipe()
	require.NoError(t, err)
	dst.Topic = "dbserver1"
	dst.FormatSettings = model.SerializationFormat{Name: model.SerializationFormatDebezium}
	//------------------------------------------------------------------------------
	// prepare additional transfer: from dst to mock

	result := make([]abstract.ChangeItem, 0)
	mockSink := mocksink.NewMockSink(func(in []abstract.ChangeItem) error {
		abstract.Dump(in)
		result = append(result, in...)
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
	//------------------------------------------------------------------------------
	// activate main transfer

	transferhelpers.InitSrcDst(transferhelpers.TransferID, Source, dst, abstract.TransferTypeSnapshotOnly)
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, Source, dst, abstract.TransferTypeSnapshotOnly)

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

	for {
		if len(result) == 1 {
			vv, _ := changeitem.GetRawMessageData(result[0])
			canonVal := eraseMeta(string(vv))
			canon.SaveJSON(t, canon2.AddIndentToJSON(t, canonVal))
			break
		}
		time.Sleep(time.Second)
	}
}
