package snapshot

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/debezium"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
	"github.com/transferia/transferia/pkg/debezium/testutil"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/tests/helpers"
	yt_helpers "github.com/transferia/transferia/tests/helpers/yt"
)

var (
	Source = postgres.PgSource{
		ClusterID: os.Getenv("PG_CLUSTER_ID"),
		Host:      "localhost",
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      helpers.GetIntFromEnv("PG_LOCAL_PORT"),
		DBTables:  []string{"public.__test"},
	}
	Target = yt_helpers.RecipeYtTarget("//home/cdc/test/pg2yt_e2e")
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

//---------------------------------------------------------------------------------------------------------------------

var countOfProcessedMessage = 0

func makeDebeziumSerDeUdf(emitter *debezium.Emitter, receiver *debezium.Receiver) helpers.SimpleTransformerApplyUDF {
	return func(t *testing.T, items []abstract.ChangeItem) abstract.TransformerResult {
		newChangeItems := make([]abstract.ChangeItem, 0)
		for i := range items {
			if items[i].IsSystemTable() {
				continue
			}
			if items[i].Kind == abstract.InsertKind {
				countOfProcessedMessage++
				fmt.Printf("changeItem dump: %s\n", items[i].ToJSONString())
				resultKV, err := emitter.EmitKV(&items[i], time.Time{}, true, nil)
				require.NoError(t, err)
				for _, debeziumKV := range resultKV {
					fmt.Printf("debeziumMsg dump: %s\n", *debeziumKV.DebeziumVal)
					changeItem, err := receiver.Receive(*debeziumKV.DebeziumVal)
					require.NoError(t, err)
					fmt.Printf("changeItem received dump: %s\n", changeItem.ToJSONString())
					newChangeItems = append(newChangeItems, *changeItem)

					testutil.CompareYTTypesOriginalAndRecovered(t, &items[i], changeItem)
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

func anyTablesUdf(table abstract.TableID, schema abstract.TableColumns) bool {
	return true
}

//---------------------------------------------------------------------------------------------------------------------

func TestGroup(t *testing.T) {
	targetPort, err := helpers.GetPortFromStr(Target.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
			helpers.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Snapshot", Snapshot)
	})
}

func Snapshot(t *testing.T) {
	Source.PreSteps.Constraint = true
	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotOnly)

	emitter, err := debezium.NewMessagesEmitter(map[string]string{
		debeziumparameters.DatabaseDBName:   "public",
		debeziumparameters.TopicPrefix:      "my_topic",
		debeziumparameters.AddOriginalTypes: "true",
		debeziumparameters.SourceType:       "pg",
	}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	receiver := debezium.NewReceiver(nil, nil)
	debeziumSerDeTransformer := helpers.NewSimpleTransformer(t, makeDebeziumSerDeUdf(emitter, receiver), anyTablesUdf)
	helpers.AddTransformer(t, transfer, debeziumSerDeTransformer)

	_ = helpers.Activate(t, transfer)

	require.NoError(t, helpers.CompareStorages(t, Source, Target.LegacyModel(), helpers.NewCompareStorageParams()))
}
