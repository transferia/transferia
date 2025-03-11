package serde

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/debezium"
	simple_transformer "github.com/transferria/transferria/tests/helpers/transformer"
)

func MakeYdb2YdbDebeziumSerDeUdf(pathOut string, outLastInsert *abstract.ChangeItem, emitter *debezium.Emitter, receiver *debezium.Receiver) simple_transformer.SimpleTransformerApplyUDF {
	return func(t *testing.T, items []abstract.ChangeItem) abstract.TransformerResult {
		newChangeItems := make([]abstract.ChangeItem, 0)
		for i := range items {
			items[i].Table = pathOut
			if items[i].Kind == abstract.InsertKind && outLastInsert != nil {
				*outLastInsert = items[i]
			}
			if items[i].Kind == abstract.InsertKind || items[i].Kind == abstract.UpdateKind || items[i].Kind == abstract.DeleteKind {
				logger.Log.Infof("changeItem dump: %s\n", items[i].ToJSONString())
				resultKV, err := emitter.EmitKV(&items[i], time.Time{}, true, nil)
				require.NoError(t, err)
				for _, debeziumKV := range resultKV {
					logger.Log.Infof("debeziumMsg dump: %s\n", *debeziumKV.DebeziumVal)
					changeItem, err := receiver.Receive(*debeziumKV.DebeziumVal)
					require.NoError(t, err)
					logger.Log.Infof("changeItem received dump: %s\n", changeItem.ToJSONString())
					newChangeItems = append(newChangeItems, *changeItem)
				}
			} else {
				newChangeItems = append(newChangeItems, items[i])
			}
		}
		return abstract.TransformerResult{
			Transformed: newChangeItems,
			Errors:      nil,
		}
	}
}
