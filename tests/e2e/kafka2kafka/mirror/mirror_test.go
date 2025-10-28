package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	kafkasink "github.com/transferia/transferia/pkg/providers/kafka"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/tests/helpers"
	"go.ytsaurus.tech/library/go/core/log"
)

func TestReplication(t *testing.T) {
	srcTopic := "topic1"
	dstTopic := "topic2"

	src, err := kafkasink.SourceRecipe()
	require.NoError(t, err)
	src.Topic = srcTopic

	dst, err := kafkasink.DestinationRecipe()
	require.NoError(t, err)
	dst.Topic = dstTopic
	dst.FormatSettings = model.SerializationFormat{Name: model.SerializationFormatMirror}

	// write to source topic

	k := []byte(`my_key`)
	v := []byte(`blablabla`)

	srcSink, err := kafkasink.NewReplicationSink(
		&kafkasink.KafkaDestination{
			Connection:          src.Connection,
			Auth:                src.Auth,
			Topic:               src.Topic,
			FormatSettings:      dst.FormatSettings,
			ParralelWriterCount: 10,
		},
		solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()}),
		logger.Log,
	)
	require.NoError(t, err)
	err = srcSink.Push([]abstract.ChangeItem{abstract.MakeRawMessage(k, srcTopic, time.Time{}, srcTopic, 0, 0, v)})
	require.NoError(t, err)

	// prepare additional transfer: from dst to mock

	result := make([]abstract.ChangeItem, 0)
	mockSink := &helpers.MockSink{
		PushCallback: func(in []abstract.ChangeItem) error {
			abstract.Dump(in)
			result = append(result, in...)
			return nil
		},
	}
	mockTarget := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return mockSink },
		Cleanup:       model.DisabledCleanup,
	}
	additionalTransfer := helpers.MakeTransfer("additional", &kafkasink.KafkaSource{
		Connection:  dst.Connection,
		Auth:        dst.Auth,
		GroupTopics: []string{dst.Topic},
	}, &mockTarget, abstract.TransferTypeIncrementOnly)

	// activate main transfer

	helpers.InitSrcDst(helpers.TransferID, src, dst, abstract.TransferTypeIncrementOnly)
	transfer := helpers.MakeTransfer(helpers.TransferID, src, dst, abstract.TransferTypeIncrementOnly)

	localWorker := local.NewLocalWorker(coordinator.NewFakeClient(), transfer, solomon.NewRegistry(solomon.NewRegistryOpts()), log.With(logger.Log, log.Any("transfer", "main")))
	localWorker.Start()
	defer localWorker.Stop()

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

	st := time.Now()
	for time.Since(st) < time.Second*30 {
		if len(result) == 1 {
			kk, _ := changeitem.GetSequenceKey(&result[0])
			vv, _ := changeitem.GetRawMessageData(result[0])

			require.Equal(t, k, kk)
			require.Equal(t, v, vv)
			break
		}

		time.Sleep(time.Second)
	}
}
