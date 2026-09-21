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
	debezium_parameters "github.com/transferia/transferia/pkg/debezium/parameters"
	debezium_testutil "github.com/transferia/transferia/pkg/debezium/testutil"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/testenv"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	transformerhelpers "github.com/transferia/transferia/tests/helpers/transformer"
	helpers_yt "github.com/transferia/transferia/tests/helpers/yt"
)

var (
	Source = provider_postgres.PgSource{
		ClusterID: os.Getenv("PG_CLUSTER_ID"),
		Host:      "localhost",
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      testenv.GetIntFromEnv("PG_LOCAL_PORT"),
		DBTables:  []string{"public.__test"},
	}
	Target = helpers_yt.RecipeYtTarget("//home/cdc/test/pg2yt_e2e")
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
}

//---------------------------------------------------------------------------------------------------------------------

var countOfProcessedMessage = 0

func makeDebeziumSerDeUdf(emitter *debezium.Emitter, receiver *debezium.Receiver) transformerhelpers.SimpleTransformerApplyUDF {
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

					debezium_testutil.CompareYTTypesOriginalAndRecovered(t, &items[i], changeItem)
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
	targetPort, err := network.GetPortFromStr(Target.Cluster())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, network.CheckConnections(
			network.LabeledPort{Label: "PG source", Port: Source.Port},
			network.LabeledPort{Label: "YT target", Port: targetPort},
		))
	}()

	t.Run("Group after port check", func(t *testing.T) {
		t.Run("Snapshot", Snapshot)
	})
}

func Snapshot(t *testing.T) {
	Source.PreSteps.Constraint = true
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, Target, abstract.TransferTypeSnapshotOnly)

	emitter, err := debezium.NewMessagesEmitter(map[string]string{
		debezium_parameters.DatabaseDBName:   "public",
		debezium_parameters.TopicPrefix:      "my_topic",
		debezium_parameters.AddOriginalTypes: "true",
		debezium_parameters.SourceType:       "pg",
	}, "1.1.2.Final", false, logger.Log)
	require.NoError(t, err)
	receiver := debezium.NewReceiver(nil, nil)
	debeziumSerDeTransformer := transformerhelpers.NewSimpleTransformer(t, makeDebeziumSerDeUdf(emitter, receiver), anyTablesUdf)
	transformerhelpers.AddTransformer(t, transfer, debeziumSerDeTransformer)

	_ = delivery.Activate(t, transfer)

	require.NoError(t, storagecomparison.CompareStorages(t, Source, Target.LegacyModel(), storagecomparison.NewCompareStorageParams()))
}
