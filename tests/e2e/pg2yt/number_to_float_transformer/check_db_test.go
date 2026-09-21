package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	_ "github.com/transferia/transferia/pkg/transformer/registry/number_to_float"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/network"
	_ "github.com/transferia/transferia/tests/helpers/registration/postgres"
	_ "github.com/transferia/transferia/tests/helpers/registration/yt"
	"github.com/transferia/transferia/tests/helpers/storage"
	"github.com/transferia/transferia/tests/helpers/storage/storagecomparison"
	"github.com/transferia/transferia/tests/helpers/transfer"
	helpers_yt "github.com/transferia/transferia/tests/helpers/yt"
)

var (
	transferType = abstract.TransferTypeSnapshotAndIncrement
	source       = pgrecipe.RecipeSource()
	target       = helpers_yt.RecipeYtTarget("//home/cdc/test/pg2yt_e2e")

	waitTimeout = 300 * time.Second
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	source.WithDefaults()
}

func TestSnapshotAndReplication(t *testing.T) {
	targetPort, err := network.GetPortFromStr(target.Cluster())
	require.NoError(t, err)

	defer require.NoError(t, network.CheckConnections(
		network.LabeledPort{Label: "PG source", Port: source.Port},
		network.LabeledPort{Label: "YT target", Port: targetPort},
	))

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, source, target, transferType)

	require.NoError(t, transfer.TransformationFromJSON(`
{
    "transformers": [
      {
        "numberToFloatTransformer": {
          "tables": {
            "includeTables": [
                "^public.test$"
            ]
          }
        }
      }
    ]
}
`))

	worker := delivery.Activate(t, transfer)
	defer worker.Close(t)

	t.Run("Snapshot", Snapshot)

	t.Run("Replication", Replication)

	t.Run("Canon", Canon)
}

func Snapshot(t *testing.T) {
	dst := storagecomparison.GetSampleableStorageByModel(t, target)
	n := uint64(1)
	require.NoError(t, storage.WaitDestinationEqualRowsCount("public", "test", dst, waitTimeout, n))
	require.NoError(t, storage.WaitDestinationEqualRowsCount("public", "test_not_transformed", dst, waitTimeout, n))
}

func Replication(t *testing.T) {
	inputMap := map[string]interface{}{
		"k1": 234.56,
		"k2": []interface{}{123.45},
		"k3": map[string]interface{}{
			"123.45": 234.56,
			"key":    "123.321",
		},
		"k4": "123.123",
	}

	inputSlice := []interface{}{
		234.56,
		[]interface{}{123.45},
		map[string]interface{}{
			"123.45": 234.56,
			"key":    "123.321",
		},
		"123.123",
	}

	toQuery := []interface{}{
		inputSlice,
		[]interface{}{
			432.85,
			inputSlice,
			inputMap,
		},
		[]interface{}{
			inputSlice,
			inputMap,
			123.45,
		},
		map[string]interface{}{
			"key1": 854.213,
			"key2": inputSlice,
			"key3": inputMap,
			"key4": map[string]interface{}{
				"key1": 854.213,
				"key2": inputSlice,
				"key3": inputMap,
			},
			"key5": []interface{}{
				423.124,
				"2353.2345",
				234.234,
				inputSlice,
				inputMap,
			},
		},
		[]interface{}{
			999.111,
			inputMap,
			inputSlice,
			[]interface{}{
				423.124,
				"2353.2345",
				234.234,
				inputSlice,
				inputMap,
			},
			map[string]interface{}{
				"key1": 854.213,
				"key2": inputSlice,
				"key3": inputMap,
			},
		},
	}

	replicationQuery := getReplicationQuery(t, toQuery)

	// Also test processing of UPDATE items.
	replicationQuery += `
		INSERT INTO test(i, j, jb) VALUES (
			100,               -- i
			'{"key": 100.01}', -- j
			'{"key": 100.01}'  -- jb
		);
		INSERT INTO test_not_transformed(i, j, jb) VALUES (
			100,               -- i
			'{"key": 100.01}', -- j
			'{"key": 100.01}'  -- jb
		);
		UPDATE test SET j = '{"key": 999.99}', jb = '{"key": 999.99}' WHERE i = 100;
		UPDATE test_not_transformed SET j = '{"key": 999.99}', jb = '{"key": 999.99}' WHERE i = 100;`

	srcConn, err := provider_postgres.MakeConnPoolFromSrc(source, logger.Log)
	require.NoError(t, err)
	_, err = srcConn.Exec(context.Background(), replicationQuery)
	srcConn.Close()
	require.NoError(t, err)

	dst := storagecomparison.GetSampleableStorageByModel(t, target)
	n := uint64(len(toQuery)) + 2 // +2 because we have 1 row from snapshot and 1 row with update
	require.NoError(t, storage.WaitDestinationEqualRowsCount("public", "test", dst, waitTimeout, n))
	require.NoError(t, storage.WaitDestinationEqualRowsCount("public", "test_not_transformed", dst, waitTimeout, n))
}

func Canon(t *testing.T) {
	dst := storagecomparison.GetSampleableStorageByModel(t, target)

	var resWithNumbers []abstract.ChangeItem
	desc := abstract.TableDescription{Schema: "public", Name: "test_not_transformed"}
	require.NoError(t, dst.LoadTable(context.Background(), desc, func(items []abstract.ChangeItem) error {
		for i := range items {
			items[i].CommitTime = 0
		}
		resWithNumbers = append(resWithNumbers, items...)
		return nil
	}))

	var resWithFloats []abstract.ChangeItem
	desc = abstract.TableDescription{Schema: "public", Name: "test"}
	require.NoError(t, dst.LoadTable(context.Background(), desc, func(items []abstract.ChangeItem) error {
		for i := range items {
			items[i].CommitTime = 0
		}
		resWithFloats = append(resWithFloats, items...)
		return nil
	}))

	canon.SaveJSON(t, map[string]interface{}{"numbers": resWithNumbers, "floats": resWithFloats})
}

func getReplicationQuery(t *testing.T, data []interface{}) string {
	res := strings.Builder{}
	for _, elem := range data {
		jsonBytes, err := json.Marshal(elem)
		require.NoError(t, err)
		res.WriteString(fmt.Sprintf(`
			INSERT INTO test(j, jb) VALUES (
				'%[1]s', -- j
				'%[1]s'  -- jb
			);
			INSERT INTO test_not_transformed(j, jb) VALUES (
				'%[1]s', -- j
				'%[1]s'  -- jb
			);`, string(jsonBytes),
		))
	}
	return res.String()
}
