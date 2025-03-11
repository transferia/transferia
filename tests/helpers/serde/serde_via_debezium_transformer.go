package serde

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/debezium"
	"github.com/transferria/transferria/pkg/debezium/testutil"
	simple_transformer "github.com/transferria/transferria/tests/helpers/transformer"
)

var CountOfProcessedMessage = 0

func makeDebeziumSerDeUdf(emitter *debezium.Emitter, receiver *debezium.Receiver, checkYtTypes bool) simple_transformer.SimpleTransformerApplyUDF {
	return func(t *testing.T, items []abstract.ChangeItem) abstract.TransformerResult {
		newChangeItems := make([]abstract.ChangeItem, 0)
		for i := range items {
			if items[i].IsSystemTable() {
				continue
			}
			if items[i].Kind == abstract.InsertKind || items[i].Kind == abstract.UpdateKind || items[i].Kind == abstract.DeleteKind {
				CountOfProcessedMessage++
				fmt.Printf("changeItem dump: %s\n", items[i].ToJSONString())
				resultKV, err := emitter.EmitKV(&items[i], time.Time{}, true, nil)
				require.NoError(t, err)
				for _, debeziumKV := range resultKV {
					fmt.Printf("debeziumMsg dump: %s\n", *debeziumKV.DebeziumVal)
					changeItem, err := receiver.Receive(*debeziumKV.DebeziumVal)
					require.NoError(t, err)
					fmt.Printf("changeItem received dump: %s\n", changeItem.ToJSONString())
					newChangeItems = append(newChangeItems, *changeItem)

					if checkYtTypes {
						testutil.CompareYTTypesOriginalAndRecovered(t, &items[i], changeItem)
					}
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

func MakeDebeziumSerDeUdfWithCheck(emitter *debezium.Emitter, receiver *debezium.Receiver) simple_transformer.SimpleTransformerApplyUDF {
	return makeDebeziumSerDeUdf(emitter, receiver, true)
}

func MakeDebeziumSerDeUdfWithoutCheck(emitter *debezium.Emitter, receiver *debezium.Receiver) simple_transformer.SimpleTransformerApplyUDF {
	return makeDebeziumSerDeUdf(emitter, receiver, false)
}

func AnyTablesUdf(table abstract.TableID, schema abstract.TableColumns) bool {
	return true
}
