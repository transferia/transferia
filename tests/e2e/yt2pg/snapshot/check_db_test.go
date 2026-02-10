package snapshot

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
	yt_provider "github.com/transferia/transferia/pkg/providers/yt"
	ytclient "github.com/transferia/transferia/pkg/providers/yt/client"
	"github.com/transferia/transferia/tests/helpers"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

var (
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = yt_provider.YtSource{
		Cluster:          os.Getenv("YT_PROXY"),
		YtProxy:          os.Getenv("YT_PROXY"),
		Paths:            []string{"//home/cdc/junk/test_table"},
		YtToken:          "",
		RowIdxColumnName: "row_idx",
	}
	dstPort, _ = strconv.Atoi(os.Getenv("PG_LOCAL_PORT"))
	Target     = postgres.PgDestination{
		Hosts:     []string{"localhost"},
		ClusterID: os.Getenv("TARGET_CLUSTER_ID"),
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      dstPort,
		Cleanup:   model.Truncate,
	}
)

func init() {
	_ = os.Setenv("YC", "1")                                               // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

var TestData = []map[string]interface{}{
	{
		"t_int8":      0,
		"t_int16":     -1000,
		"t_int32":     -100000,
		"t_int64":     -10000000000,
		"t_uint8":     10,
		"t_uint16":    1000,
		"t_uint32":    1000000,
		"t_uint64":    10000000000,
		"t_float":     float32(1.2),
		"t_double":    1.2,
		"t_bool":      false,
		"t_string":    []byte("Test byte string 1"),
		"t_utf8":      "Test utf8 string 1",
		"t_date":      1640604030 / (24 * 60 * 60),
		"t_datetime":  1640604030,
		"t_timestamp": 1640604030502383,
		// Interval:  -10000000,
		"t_yson": map[string]uint64{"test_key": 100},
		// OptInt64:  &optint,
	},
	{
		"t_int8":      1,
		"t_int16":     -2000,
		"t_int32":     -200000,
		"t_int64":     -20000000000,
		"t_uint8":     20,
		"t_uint16":    2000,
		"t_uint32":    2000000,
		"t_uint64":    20000000000,
		"t_float":     float32(2.2),
		"t_double":    2.2,
		"t_bool":      true,
		"t_string":    []byte("Test byte string 2"),
		"t_utf8":      "Test utf8 string 2",
		"t_date":      1640604030 / (24 * 60 * 60),
		"t_datetime":  1640604030,
		"t_timestamp": 1640604030502383,
		// Interval:  -10000000,
		"t_yson": []uint64{100, 200, 300},
		// OptInt64:  &optint,
	},
	{
		"t_int8":      2,
		"t_int16":     -3000,
		"t_int32":     -300000,
		"t_int64":     -30000000000,
		"t_uint8":     30,
		"t_uint16":    3000,
		"t_uint32":    3000000,
		"t_uint64":    30000000000,
		"t_float":     float32(2.7182818),
		"t_double":    2.7182818284590,
		"t_bool":      true,
		"t_string":    nil,
		"t_utf8":      "Test utf8 string 3",
		"t_date":      1640604030 / (24 * 60 * 60),
		"t_datetime":  1640604030,
		"t_timestamp": 1640604030502383,
		// Interval:  -10000000,
		"t_yson": []uint64{100, 200, 300},
		// OptInt64:  &optint,
	},
}

var YtColumns = []schema.Column{
	// Primitives
	{Name: "t_int8", ComplexType: schema.TypeInt8, SortOrder: schema.SortAscending},
	{Name: "t_int16", ComplexType: schema.TypeInt16},
	{Name: "t_int32", ComplexType: schema.TypeInt32},
	{Name: "t_int64", ComplexType: schema.TypeInt64},
	{Name: "t_uint8", ComplexType: schema.TypeUint8},
	{Name: "t_uint16", ComplexType: schema.TypeUint16},
	{Name: "t_uint32", ComplexType: schema.TypeUint32},
	{Name: "t_uint64", ComplexType: schema.TypeUint64},
	{Name: "t_float", ComplexType: schema.TypeFloat32},
	{Name: "t_double", ComplexType: schema.TypeFloat64},
	{Name: "t_bool", ComplexType: schema.TypeBoolean},
	{Name: "t_string", ComplexType: schema.Optional{Item: schema.TypeBytes}},
	{Name: "t_utf8", ComplexType: schema.TypeString},
	{Name: "t_date", ComplexType: schema.TypeDate},
	{Name: "t_datetime", ComplexType: schema.TypeDatetime},
	{Name: "t_timestamp", ComplexType: schema.TypeTimestamp},
	// {Name: "t_interval", ComplexType: schema.TypeInterval}, FIXME: support in CH
	{Name: "t_yson", ComplexType: schema.Optional{Item: schema.TypeAny}},
	// {Name: "t_opt_int64", ComplexType: schema.Optional{Item: schema.TypeInt64}},
}

func createTestData(t *testing.T) {
	ytc, err := ytclient.NewYtClientWrapper(ytclient.HTTP, nil, &yt.Config{Proxy: Source.YtProxy})
	require.NoError(t, err)

	sch := schema.Schema{
		Strict:     nil,
		UniqueKeys: false,
		Columns:    YtColumns,
	}

	ctx := context.Background()
	wr, err := yt.WriteTable(ctx, ytc, ypath.NewRich(Source.Paths[0]).YPath(), yt.WithCreateOptions(yt.WithSchema(sch), yt.WithRecursive()))
	require.NoError(t, err)
	// var optint int64 = 10050
	for _, row := range TestData {
		require.NoError(t, wr.Write(row))
	}
	require.NoError(t, wr.Commit())
}

func checkDataRow(t *testing.T, pgRow map[string]interface{}, testRow map[string]interface{}, rowKey int16, typeSystemVersion int) {
	for k, v := range testRow {
		pgValRaw := pgRow[k]
		switch k {
		case "t_datetime":
			pgVal, ok := pgValRaw.(time.Time)
			require.Truef(t, ok, "expected %s to be %T, got %T", k, pgVal, pgValRaw)
			require.Equal(t, int64(v.(int)), pgVal.Unix())
		case "t_timestamp":
			pgVal, ok := pgValRaw.(time.Time)
			require.Truef(t, ok, "expected %s to be %T, got %T", k, pgVal, pgValRaw)
			require.Equal(t, int64(v.(int)), pgVal.UnixNano()/1000)
		case "t_date":
			pgVal, ok := pgValRaw.(time.Time)
			require.Truef(t, ok, "expected %s to be %T, got %T", k, pgVal, pgValRaw)
			testVal := int64(v.(int) * (24 * 60 * 60))
			require.Equal(t, testVal, pgVal.Unix())
		case "t_float":
			pgVal, ok := pgValRaw.(json.Number)
			require.Truef(t, ok, "expected %s to be %T, got %T", k, pgVal, pgValRaw)
			pgValF, err := pgVal.Float64()
			require.NoError(t, err)
			vF, ok := v.(float32)
			require.True(t, ok)
			require.Equal(t, vF, float32(pgValF))
		case "t_double":
			pgVal, ok := pgValRaw.(json.Number)
			require.Truef(t, ok, "expected %s to be %T, got %T", k, pgVal, pgValRaw)
			pgValF, err := pgVal.Float64()
			require.NoError(t, err)
			vF, ok := v.(float64)
			require.True(t, ok)
			require.Equal(t, vF, pgValF)
		case "t_yson":
			switch rowKey {
			case 0:
				pgVal, ok := pgValRaw.(map[string]interface{})
				require.Truef(t, ok, "expected %s to be %T, got %T", k, pgVal, pgValRaw)
				require.Equal(t, json.Number("100"), pgVal["test_key"])
			case 1, 2:
				pgVal, ok := pgValRaw.([]interface{})
				require.Truef(t, ok, "expected %s to be %T, got %T", k, pgVal, pgValRaw)
				for i, pgJSONArrayItem := range pgVal {
					vv := v.([]uint64)[i]
					pgJSONInt64, err := pgJSONArrayItem.(json.Number).Int64()
					require.NoError(t, err)
					require.Equal(t, int64(vv), pgJSONInt64)
				}
			default:
				require.Fail(t, "unknown row key", "row key %d", rowKey)
			}
		case "t_string":
			if typeSystemVersion != 0 && typeSystemVersion < 8 {
				require.EqualValues(t, v, pgValRaw, "non-matching values for column %s (pg type %T)", k, pgValRaw)
			} else {
				if v == nil || pgValRaw == nil {
					require.EqualValues(t, v, pgValRaw, "non-matching values for column %s (pg type %T)", k, pgValRaw)
				} else {
					pgVal, ok := pgValRaw.(string)
					require.Truef(t, ok, "expected %s to be %T, got %T", k, pgVal, pgValRaw)
					pgValDecoded, err := hex.DecodeString(strings.TrimPrefix(pgVal, `\x`))
					require.NoError(t, err)
					require.Equal(t, v.([]byte), pgValDecoded)
				}
			}
		default:
			require.EqualValues(t, v, pgValRaw, "non-matching values for column %s (pg type %T)", k, pgValRaw)
		}
	}
}

func doSnapshotForTSV(t *testing.T, typeSystemVersion int) {
	testName := fmt.Sprintf("TypeSystemVersion=%d", typeSystemVersion)
	if typeSystemVersion == 0 {
		testName = "TypeSystemVersion=default"
	}

	t.Run(testName, func(t *testing.T) {
		transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
		if typeSystemVersion != 0 {
			transfer.TypeSystemVersion = typeSystemVersion
		}
		worker := helpers.Activate(t, transfer)
		defer worker.Close(t)

		pgTarget := helpers.GetSampleableStorageByModel(t, Target)
		totalInserts := 0
		require.NoError(t, pgTarget.LoadTable(context.Background(), abstract.TableDescription{
			Name:   "test_table",
			Schema: "public",
		}, func(input []abstract.ChangeItem) error {
			for _, ci := range input {
				if ci.Kind != abstract.InsertKind {
					continue
				}
				pgRow := ci.AsMap()
				keyRaw, ok := pgRow["t_int8"]
				if !ok {
					require.Fail(t, "faulty test: missing key column")
				}
				key, ok := keyRaw.(int16)
				if !ok {
					require.Fail(t, "key column is of wrong type", "wrong type %T", keyRaw)
				}
				checkDataRow(t, pgRow, TestData[key], key, typeSystemVersion)
				totalInserts += 1
			}
			return nil
		}))

		require.Equal(t, len(TestData), totalInserts)
	})
}

func TestSnapshot(t *testing.T) {
	// defer require.NoError(t, helpers.CheckConnections(, Target.NativePort))
	createTestData(t)

	doSnapshotForTSV(t, 0)
	doSnapshotForTSV(t, 4)
}
