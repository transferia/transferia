package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/library/go/core/metrics/solomon"
	"github.com/transferria/transferria/library/go/test/canon"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/coordinator"
	"github.com/transferria/transferria/pkg/abstract/model"
	kafkasink "github.com/transferria/transferria/pkg/providers/kafka"
	"github.com/transferria/transferria/pkg/runtime/local"
	"github.com/transferria/transferria/tests/helpers"
)

func TestReplication(t *testing.T) {
	src, err := kafkasink.SourceRecipe()
	require.NoError(t, err)
	src.IsHomo = true

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
	mockSink := &helpers.MockSink{
		PushCallback: func(in []abstract.ChangeItem) {
			result = append(result, in...)
		},
	}
	mockTarget := model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return mockSink },
		Cleanup:       model.DisabledCleanup,
	}
	additionalTransfer := helpers.MakeTransfer("additional", &kafkasink.KafkaSource{
		Connection:  dst.Connection,
		Auth:        dst.Auth,
		GroupTopics: []string{"topic1", "topic2"},
		IsHomo:      true,
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
		readedData[ci.TableID().String()] = map[string]string{
			"key":  string(kafkasink.GetKafkaRawMessageKey(&ci)),
			"data": string(kafkasink.GetKafkaRawMessageData(&ci)),
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
	err = srcSink.Push([]abstract.ChangeItem{kafkasink.MakeKafkaRawMessage(srcTopic, time.Time{}, srcTopic, 0, 0, k, v)})
	require.NoError(t, err)
	require.NoError(t, srcSink.Close())
}
