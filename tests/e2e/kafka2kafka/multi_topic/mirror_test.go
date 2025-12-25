package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	kafkasink "github.com/transferia/transferia/pkg/providers/kafka"
	"github.com/transferia/transferia/pkg/runtime/local"
	"github.com/transferia/transferia/tests/helpers"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
)

func TestReplication(t *testing.T) {
	src, err := kafkasink.SourceRecipe()
	require.NoError(t, err)

	dst, err := kafkasink.DestinationRecipe()
	require.NoError(t, err)
	dst.FormatSettings = model.SerializationFormat{Name: model.SerializationFormatMirror}

	// write to source topic
	k := []byte(`my_key`)
	v := []byte(`blablabla`)

	pushData(t, *src, "topic1", *dst, k, v)
	pushData(t, *src, "topic2", *dst, k, v)

	// prepare additional transfer: from dst to mock

	result := make([]abstract.ChangeItem, 0)
	mockSink := mocksink.NewMockSink(func(in []abstract.ChangeItem) error {
		result = append(result, in...)
		return nil
	})
	mockTarget := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return mockSink },
		Cleanup:       model.DisabledCleanup,
	}
	additionalTransfer := helpers.MakeTransfer("additional", &kafkasink.KafkaSource{
		Connection:  dst.Connection,
		Auth:        dst.Auth,
		GroupTopics: []string{"topic1", "topic2"},
	}, &mockTarget, abstract.TransferTypeIncrementOnly)

	localAdditionalWorker := local.NewLocalWorker(coordinator.NewFakeClient(), additionalTransfer, solomon.NewRegistry(solomon.NewRegistryOpts()), logger.Log)
	localAdditionalWorker.Start()
	defer localAdditionalWorker.Stop()

	//-----------------------------------------------------------------------------------------------------------------

	st := time.Now()
	for time.Since(st) < time.Minute {
		if len(result) < 2 {
			time.Sleep(time.Second)
			continue
		}
		break
	}
	readedData := map[string]map[string]string{}
	for _, ci := range result {
		kk, _ := changeitem.GetSequenceKey(&ci)
		vv, _ := changeitem.GetRawMessageData(ci)

		readedData[ci.TableID().String()] = map[string]string{
			"key":  string(kk),
			"data": string(vv),
		}
	}
	require.Len(t, result, 2)
	canon.SaveJSON(t, readedData)
}

func pushData(t *testing.T, src kafkasink.KafkaSource, srcTopic string, dst kafkasink.KafkaDestination, k []byte, v []byte) {
	srcSink, err := kafkasink.NewReplicationSink(
		&kafkasink.KafkaDestination{
			Connection:          src.Connection,
			Auth:                src.Auth,
			Topic:               srcTopic,
			FormatSettings:      dst.FormatSettings,
			ParralelWriterCount: 10,
		},
		solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()}),
		logger.Log,
	)
	require.NoError(t, err)
	err = srcSink.Push([]abstract.ChangeItem{abstract.MakeRawMessage(k, srcTopic, time.Time{}, srcTopic, 0, 0, v)})
	require.NoError(t, err)
	require.NoError(t, srcSink.Close())
}
