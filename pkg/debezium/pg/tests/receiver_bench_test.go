package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/debezium"
	debeziumcommon "github.com/transferria/transferria/pkg/debezium/common"
	debeziumparameters "github.com/transferria/transferria/pkg/debezium/parameters"
	"go.uber.org/zap/zapcore"
)

func BenchmarkReceiverTest(b *testing.B) {
	b.Run("parse", func(b *testing.B) {
		b.Run("tz", func(b *testing.B) {
			canonDebeziumMsgWithoutSequence := wipeSequenceAndIncremental(debeziumMsg30)
			receiver := debezium.NewReceiver(map[abstract.TableID]map[string]*debeziumcommon.OriginalTypeInfo{
				{Namespace: "public", Name: "basic_types"}: {
					"id":  {OriginalType: "pg:integer"},
					"val": {OriginalType: `pg:timestamp without time zone`, Properties: map[string]string{"timezone": "Europe/Moscow"}},
				},
			}, nil)

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				co, err := receiver.Receive(canonDebeziumMsgWithoutSequence)
				require.NoError(b, err)
				b.SetBytes(int64(co.Size.Values))
			}
			b.ReportAllocs()
		})
		b.Run("no-tz", func(b *testing.B) {
			canonDebeziumMsgWithoutSequence := wipeSequenceAndIncremental(debeziumMsg30)
			receiver := debezium.NewReceiver(map[abstract.TableID]map[string]*debeziumcommon.OriginalTypeInfo{
				{Namespace: "public", Name: "basic_types"}: {
					"id":  {OriginalType: "pg:integer"},
					"val": {OriginalType: `pg:timestamp without time zone`},
				},
			}, nil)

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				co, err := receiver.Receive(canonDebeziumMsgWithoutSequence)
				require.NoError(b, err)
				b.SetBytes(int64(co.Size.Values))
			}
			b.ReportAllocs()
		})
	})
	b.Run("serialize", func(b *testing.B) {
		changeItem := extractCI(b, debeziumMsg31, map[abstract.TableID]map[string]*debeziumcommon.OriginalTypeInfo{
			{Namespace: "public", Name: "basic_types"}: {
				"id":  {OriginalType: "pg:integer"},
				"val": {OriginalType: `pg:timestamp with time zone`},
			},
		})
		emitter, err := debezium.NewMessagesEmitter(map[string]string{
			debeziumparameters.DatabaseDBName:   "pguser",
			debeziumparameters.TopicPrefix:      "fullfillment",
			debeziumparameters.AddOriginalTypes: "false",
			debeziumparameters.SourceType:       "pg",
		}, "1.8.0.Final", false, logger.LoggerWithLevel(zapcore.WarnLevel))
		require.NoError(b, err)

		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			result, err := emitter.EmitKV(&changeItem, debezium.GetPayloadTSMS(&changeItem), false, nil)
			require.NoError(b, err)
			require.Equal(b, len(result), 1)
			b.SetBytes(int64(len(*result[0].DebeziumVal)))
		}
		b.ReportAllocs()
	})
}

func extractCI(
	t require.TestingT,
	debeziumMsg string,
	originalTypes map[abstract.TableID]map[string]*debeziumcommon.OriginalTypeInfo,
) abstract.ChangeItem {
	canonDebeziumMsgWithoutSequence := wipeSequenceAndIncremental(debeziumMsg)
	receiver := debezium.NewReceiver(originalTypes, nil)
	changeItemStr, err := ReceiveStr(receiver, canonDebeziumMsgWithoutSequence)
	require.NoError(t, err)
	changeItem, err := abstract.UnmarshalChangeItem([]byte(changeItemStr))
	require.NoError(t, err)
	return *changeItem
}
