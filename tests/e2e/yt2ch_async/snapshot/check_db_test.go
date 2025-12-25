package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	dp_model "github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/model"
	yt_provider "github.com/transferia/transferia/pkg/providers/yt"
	ytclient "github.com/transferia/transferia/pkg/providers/yt/client"
	"github.com/transferia/transferia/pkg/worker/tasks"
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
	Target = model.ChDestination{
		ShardsList: []model.ClickHouseShard{
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
		Cleanup:             dp_model.Drop,
		Interval:            time.Duration(-1),
	}
)

func init() {
	_ = os.Setenv("YC", "1")                                               // to not go to vanga
	helpers.InitSrcDst(helpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

var TestData = []map[string]interface{}{
	{
		"t_int8":      -10,
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
		"t_int8":      -0,
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
		"t_int8":      10,
		"t_int16":     -3000,
		"t_int32":     -300000,
		"t_int64":     -30000000000,
		"t_uint8":     30,
		"t_uint16":    3000,
		"t_uint32":    3000000,
		"t_uint64":    30000000000,
		"t_float":     float32(math.Inf(-1)),
		"t_double":    math.NaN(),
		"t_bool":      true,
		"t_string":    []byte("Test byte string 3"),
		"t_utf8":      "Test utf8 string 3",
		"t_date":      1640604030 / (24 * 60 * 60),
		"t_datetime":  1640604030,
		"t_timestamp": 1640604030502383,
		// Interval:  -10000000,
		"t_yson": []uint64{100, 200, 300},
		// OptInt64:  &optint,
	},
	{
		"t_int8":      20,
		"t_int16":     -4000,
		"t_int32":     -400000,
		"t_int64":     -40000000000,
		"t_uint8":     40,
		"t_uint16":    4000,
		"t_uint32":    4000000,
		"t_uint64":    40000000000,
		"t_float":     float32(-273.15),
		"t_double":    351.17,
		"t_bool":      true,
		"t_string":    nil,
		"t_utf8":      "",
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

func checkSchema(t *testing.T, columns []abstract.ColSchema) {
	for _, col := range columns {
		if col.ColumnName == "row_idx" {
			require.Equal(t, "int64", col.DataType)
			require.Equal(t, true, col.PrimaryKey)
			continue
		}
		var testCol *schema.Column
		for _, c := range YtColumns {
			if c.Name == col.ColumnName {
				testCol = &c
				break
			}
		}
		require.NotNil(t, testCol)
		require.Equal(t, testCol.SortOrder != schema.SortNone, col.PrimaryKey)
		// fmt.Printf("Column %s: type %s, origType %s\n", col.ColumnName, col.DataType, col.OriginalType)
		switch col.ColumnName {
		case "t_utf8", "t_yson":
			require.EqualValues(t, "string", col.DataType)
		case "t_string":
			require.EqualValues(t, "string", col.DataType)
		case "t_float":
			require.Equal(t, "ch:Float32", col.OriginalType)
		case "t_bool":
			require.EqualValues(t, "uint8", col.DataType)
		default:
			require.EqualValuesf(t, testCol.ComplexType, col.DataType, "column %s expected type is %s, actual %s", col.ColumnName, testCol.ComplexType, col.DataType)
		}
	}
}

func checkFloatEqual(t *testing.T, v float64, chVal float64) {
	if math.IsNaN(chVal) {
		require.True(t, math.IsNaN(v))
		return
	}
	if math.IsInf(chVal, 1) {
		require.True(t, math.IsInf(v, 1))
		return
	}
	if math.IsInf(chVal, -1) {
		require.True(t, math.IsInf(v, -1))
		return
	}
	require.EqualValues(t, v, chVal)
}

func checkDataRow(t *testing.T, chRow map[string]interface{}) {
	rowIdx, ok := chRow["row_idx"].(int64)
	require.Truef(t, ok, "expected rowIdx to be %T, got %T", rowIdx, chRow["row_idx"])
	testRow := TestData[int(rowIdx)]

	for k, v := range testRow {
		chValRaw := chRow[k]
		switch k {
		case "row_idx":
			require.Equal(t, rowIdx, chValRaw)
		case "t_date":
			chVal, ok := chValRaw.(time.Time)
			require.Truef(t, ok, "expected %s to be %T, got %T", k, chVal, chValRaw)
			testVal := time.Unix(int64(v.(int)*(24*60*60)), 0)

			// driver reads Date in local CH server TZ, testVal is in UTC, make them equal
			_, tz := chVal.Zone()
			testVal = testVal.Add(-1 * time.Duration(tz) * time.Second)
			require.Truef(t, testVal.Equal(chVal), "expected %s to be equal to %s", testVal.String(), chVal.String())
		case "t_datetime":
			chVal, ok := chValRaw.(time.Time)
			require.Truef(t, ok, "expected %s to be %T, got %T", k, chVal, chValRaw)
			testVal := time.Unix(int64(v.(int)), 0)
			require.Truef(t, testVal.Equal(chVal), "expected %s to be equal to %s", testVal.String(), chVal.String())
		case "t_timestamp":
			chVal, ok := chValRaw.(time.Time)
			require.Truef(t, ok, "expected %s to be %T, got %T", k, chVal, chValRaw)
			fmt.Println(chVal.String())
			testVal := time.UnixMicro(int64(v.(int)))
			require.Truef(t, testVal.Equal(chVal), "expected %s to be equal to %s", testVal.String(), chVal.String())
		case "t_bool":
			chVal, ok := chValRaw.(uint8)
			require.Truef(t, ok, "expected %s to be %T, got %T", k, chVal, chValRaw)
			require.Equal(t, v, chVal != 0)
		case "t_yson":
			chVal, ok := chValRaw.(string)
			require.Truef(t, ok, "expected %s to be %T, got %T", k, chVal, chValRaw)
			jsv, err := json.Marshal(v)
			require.NoError(t, err)
			require.Equal(t, string(jsv), chVal)
		case "t_double":
			chVal, ok := chValRaw.(float64)
			require.Truef(t, ok, "expected %s to be %T, got %T", k, chVal, chValRaw)
			checkFloatEqual(t, v.(float64), chVal)
		case "t_float":
			chVal, ok := chValRaw.(float32)
			require.Truef(t, ok, "expected %s to be %T, got %T", k, chVal, chValRaw)
			checkFloatEqual(t, float64(v.(float32)), float64(chVal))
		case "t_string":
			chVal, ok := chValRaw.(string)
			require.Truef(t, ok, "expected %s to be %T, got %T", k, chVal, chValRaw)
			if v == nil {
				require.EqualValues(t, "", chValRaw)
			} else {
				require.EqualValues(t, v, chValRaw)
			}
		default:
			require.EqualValues(t, v, chValRaw)
		}
	}
}

func TestSnapshot(t *testing.T) {
	// defer require.NoError(t, helpers.CheckConnections(, Target.NativePort))
	createTestData(t)

	transfer := helpers.MakeTransfer(helpers.TransferID, &Source, &Target, TransferType)
	transfer.Labels = `{"dt-async-ch": "on"}`
	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), "test-operation", transfer, helpers.EmptyRegistry())
	require.NoError(t, snapshotLoader.UploadV2(context.Background(), nil, nil))

	chTarget := helpers.GetSampleableStorageByModel(t, Target)
	rowCnt := 0
	require.NoError(t, chTarget.LoadTable(context.Background(), abstract.TableDescription{
		Name:   "test_table",
		Schema: "default",
	}, func(input []abstract.ChangeItem) error {
		for _, ci := range input {
			switch ci.Kind {
			case abstract.InitTableLoad, abstract.DoneTableLoad:
				continue
			case abstract.InsertKind:
				// no need to check schema for all rows, check just once
				if rowCnt == 0 {
					checkSchema(t, ci.TableSchema.Columns())
				}
				checkDataRow(t, ci.AsMap())
				rowCnt++
			default:
				return xerrors.Errorf("unexpected ChangeItem kind %s", string(ci.Kind))
			}
		}
		return nil
	}))

	require.Equal(t, len(TestData), rowCnt)
}
