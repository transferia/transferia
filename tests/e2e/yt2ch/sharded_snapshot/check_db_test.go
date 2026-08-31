package shardedsnapshot

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/yt_client"
	"github.com/transferia/transferia/tests/helpers"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

const (
	bigTableName   = "big_table"
	bigTableRows   = 60000 // > MinShardSize (50000): the big table must shard into >=2 parts
	smallTableRows = 10
)

var smallTableNames = []string{"small_1", "small_2", "small_3"}

var (
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = provider_yt.YtSource{
		Cluster:              os.Getenv("YT_PROXY"),
		YtProxy:              os.Getenv("YT_PROXY"),
		Paths:                []string{"//home/cdc/junk/sharded_snapshot"},
		YtToken:              "",
		RowIdxColumnName:     "row_idx",
		DesiredPartSizeBytes: 1024, // small enough for the 60000-row table to shard
	}
	Target = clickhouse_model.ChDestination{
		ShardsList: []clickhouse_model.ClickHouseShard{
			{
				Name: "_",
				Hosts: []string{
					"localhost",
				},
			},
		},
		User:                "default",
		Password:            "",
		Database:            "default",
		HTTPPort:            helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_HTTP_PORT"),
		NativePort:          helpers.GetIntFromEnv("RECIPE_CLICKHOUSE_NATIVE_PORT"),
		ProtocolUnspecified: true,
		SSLEnabled:          false,
		Cleanup:             model.Drop,
		Interval:            time.Duration(-1),
	}
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType)
}

func writeTestTable(t *testing.T, name string, rows int) {
	ytc, err := yt_client.NewYtClientWrapper(yt_client.HTTP, nil, &yt.Config{Proxy: Source.YtProxy})
	require.NoError(t, err)

	sch := ytschema.Schema{
		Strict:     nil,
		UniqueKeys: false,
		Columns: []ytschema.Column{
			{Name: "key", ComplexType: ytschema.TypeInt64, SortOrder: ytschema.SortAscending},
			{Name: "value", ComplexType: ytschema.TypeInt64},
		},
	}

	ctx := context.Background()
	path := ypath.NewRich(Source.Paths[0] + "/" + name).YPath()
	wr, err := yt.WriteTable(ctx, ytc, path, yt.WithCreateOptions(yt.WithSchema(sch), yt.WithRecursive()))
	require.NoError(t, err)
	for i := 0; i < rows; i++ {
		require.NoError(t, wr.Write(map[string]interface{}{"key": int64(i), "value": int64(i * 31 % 1000003)}))
	}
	require.NoError(t, wr.Commit())
}

func checkTable(t *testing.T, chTarget abstract.Storage, name string, wantRows int) {
	t.Helper()
	got := 0
	seenKeys := make(map[int64]bool, wantRows)
	err := chTarget.LoadTable(context.Background(), abstract.TableDescription{
		Name:   name,
		Schema: "default",
	}, func(input []abstract.ChangeItem) error {
		for _, ci := range input {
			switch ci.Kind {
			case abstract.InitTableLoad, abstract.DoneTableLoad:
				continue
			case abstract.InsertKind:
				m := ci.AsMap()
				key, ok := m["key"].(int64)
				require.Truef(t, ok, "key column has unexpected type %T", m["key"])
				require.Falsef(t, seenKeys[key], "duplicate key %d", key)
				seenKeys[key] = true
				require.EqualValues(t, int64(key*31%1000003), m["value"])
				got++
			default:
				return xerrors.Errorf("unexpected ChangeItem kind %s", string(ci.Kind))
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, wantRows, got)
}

// TestShardedSnapshot runs a sharded snapshot (SnapshotWorkersNum=2) from a
// YT source with one shardable table and several small tables whose
// whole-table parts (Filter="") may land on a secondary worker. This is the
// regression scenario for the "table is not locked" failure on secondary
// workers (TM-10166 abstract1 migration).
func TestShardedSnapshot(t *testing.T) {
	writeTestTable(t, bigTableName, bigTableRows)
	for _, name := range smallTableNames {
		writeTestTable(t, name, smallTableRows)
	}

	transfer := helpers.WithLocalRuntime(
		helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType),
		2, // SnapshotWorkersNum
		1, // SnapshotThreadsNumPerWorker
	)
	_, err := helpers.ActivateShardedErr(transfer, nil, nil)
	require.NoError(t, err)

	chTarget := helpers.GetSampleableStorageByModel(t, Target)
	checkTable(t, chTarget, bigTableName, bigTableRows)
	for _, name := range smallTableNames {
		checkTable(t, chTarget, name, smallTableRows)
	}
}
