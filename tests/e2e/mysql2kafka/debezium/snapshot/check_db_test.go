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
	"github.com/transferia/transferia/pkg/abstract/model"
	kafka_provider "github.com/transferia/transferia/pkg/providers/kafka"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/tests/helpers"
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

func TestSnapshot(t *testing.T) {
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "Mysql source", Port: Source.Port},
	))
	//------------------------------------------------------------------------------
	//prepare dst

	dst, err := kafka_provider.DestinationRecipe()
	require.NoError(t, err)
	dst.Topic = "dbserver1"
	dst.FormatSettings = model.SerializationFormat{Name: model.SerializationFormatDebezium}
	//------------------------------------------------------------------------------
	// prepare additional transfer: from dst to mock

	result := make([]abstract.ChangeItem, 0)
	mockSink := &helpers.MockSink{
		PushCallback: func(in []abstract.ChangeItem) {
			abstract.Dump(in)
			result = append(result, in...)
		},
	}
	mockTarget := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return mockSink },
		Cleanup:       model.DisabledCleanup,
	}
	additionalTransfer := helpers.MakeTransfer("additional", &kafka_provider.KafkaSource{
		Connection:  dst.Connection,
		Auth:        dst.Auth,
		GroupTopics: []string{dst.Topic},
		IsHomo:      true,
	}, &mockTarget, abstract.TransferTypeIncrementOnly)
	//------------------------------------------------------------------------------
	// activate main transfer

	helpers.InitSrcDst(helpers.TransferID, Source, dst, abstract.TransferTypeSnapshotOnly)
	transfer := helpers.MakeTransfer(helpers.TransferID, Source, dst, abstract.TransferTypeSnapshotOnly)

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

	for {
		if len(result) == 1 {
			canonVal := eraseMeta(string(kafka_provider.GetKafkaRawMessageData(&result[0])))
			canon.SaveJSON(t, canonVal)
			break
		}
		time.Sleep(time.Second)
	}
}
