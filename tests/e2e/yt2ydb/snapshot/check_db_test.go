package snapshot

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_ydb "github.com/transferia/transferia/pkg/providers/ydb"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/yt_client"
	"github.com/transferia/transferia/tests/helpers"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

var (
	TransferType = abstract.TransferTypeSnapshotOnly
	Source       = provider_yt.YtSource{
		Cluster: os.Getenv("YT_PROXY"),
		YtProxy: os.Getenv("YT_PROXY"),
		Paths:   []string{"//home/cdc/junk/test_table"},
		YtToken: "",
	}
	Target = provider_ydb.YdbDestination{
		Database: os.Getenv("YDB_DATABASE"),
		Token:    model.SecretString(os.Getenv("YDB_TOKEN")),
		Instance: os.Getenv("YDB_ENDPOINT"),
	}
)

func init() {
	transferhelpers.InitSrcDst(transferhelpers.TransferID, &Source, &Target, TransferType) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
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
		"t_string":    "Test byte string 1",
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
		"t_string":    "Test byte string 2",
		"t_utf8":      "Test utf8 string 2",
		"t_date":      1640604030 / (24 * 60 * 60),
		"t_datetime":  1640604030,
		"t_timestamp": 1640604030502383,
		// Interval:  -10000000,
		"t_yson": []uint64{100, 200, 300},
		// OptInt64:  &optint,
	},
}

var YtColumns = []ytschema.Column{
	// Primitives
	{Name: "t_int8", ComplexType: ytschema.TypeInt8, SortOrder: ytschema.SortAscending},
	{Name: "t_int16", ComplexType: ytschema.TypeInt16},
	{Name: "t_int32", ComplexType: ytschema.TypeInt32},
	{Name: "t_int64", ComplexType: ytschema.TypeInt64},
	{Name: "t_uint8", ComplexType: ytschema.TypeUint8},
	{Name: "t_uint16", ComplexType: ytschema.TypeUint16},
	{Name: "t_uint32", ComplexType: ytschema.TypeUint32},
	{Name: "t_uint64", ComplexType: ytschema.TypeUint64},
	{Name: "t_float", ComplexType: ytschema.TypeFloat32},
	{Name: "t_double", ComplexType: ytschema.TypeFloat64},
	{Name: "t_bool", ComplexType: ytschema.TypeBoolean},
	{Name: "t_string", ComplexType: ytschema.TypeBytes},
	{Name: "t_utf8", ComplexType: ytschema.TypeString},
	{Name: "t_date", ComplexType: ytschema.TypeDate},
	{Name: "t_datetime", ComplexType: ytschema.TypeDatetime},
	{Name: "t_timestamp", ComplexType: ytschema.TypeTimestamp},
	// {Name: "t_interval", ComplexType: schema.TypeInterval}, FIXME: support in CH
	{Name: "t_yson", ComplexType: ytschema.Optional{Item: ytschema.TypeAny}},
	// {Name: "t_opt_int64", ComplexType: schema.Optional{Item: schema.TypeInt64}},
}

func createTestData(t *testing.T) {
	ytc, err := yt_client.NewYtClientWrapper(yt_client.HTTP, nil, &yt.Config{Proxy: Source.YtProxy})
	require.NoError(t, err)

	sch := ytschema.Schema{
		Strict:     nil,
		UniqueKeys: false,
		Columns:    YtColumns,
	}

	ctx := context.Background()
	wr, err := yt.WriteTable(ctx, ytc, ypath.NewRich(Source.Paths[0]).YPath(), yt.WithCreateOptions(yt.WithSchema(sch), yt.WithRecursive()))
	require.NoError(t, err)
	for _, row := range TestData {
		require.NoError(t, wr.Write(row))
	}
	require.NoError(t, wr.Commit())
}

func checkDataRow(t *testing.T, targetRow map[string]interface{}, testRow map[string]interface{}) {
	for k, v := range testRow {
		targetVal := targetRow[k]
		switch k {
		case "t_datetime":
			targetV, ok := targetVal.(time.Time)
			require.Truef(t, ok, "expected %s to be time.Time, got %T", k, targetV)
			require.Equal(t, int64(v.(int)), targetV.Unix())
		case "t_timestamp":
			targetV, ok := targetVal.(time.Time)
			require.Truef(t, ok, "expected %s to be time.Time, got %T", k, targetVal)
			require.Equal(t, int64(v.(int)), targetV.UnixNano()/1000)
		case "t_date":
			targetV, ok := targetVal.(time.Time)
			require.Truef(t, ok, "expected %s to be time.Time, got %T", k, targetVal)
			testVal := int64(v.(int) * (24 * 60 * 60))
			require.Equal(t, testVal, targetV.Unix())
		case "t_yson":
			targetJSON, _ := json.Marshal(targetVal)
			testJSON, _ := json.Marshal(v)
			require.EqualValues(t, string(testJSON), string(targetJSON), "non-matching values for column %s (target type %T)", k, targetVal)
		default:
			require.EqualValues(t, v, targetVal, "non-matching values for column %s (target type %T)", k, targetVal)
		}
	}
}

func TestSnapshot(t *testing.T) {
	createTestData(t)

	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, &Source, &Target, TransferType)
	worker := helpers.Activate(t, transfer)
	defer worker.Close(t)

	targetStorage := helpers.GetSampleableStorageByModel(t, Target)
	totalInserts := 0
	require.NoError(t, targetStorage.LoadTable(context.Background(), abstract.TableDescription{
		Name:   "test_table",
		Schema: "",
	}, func(input []abstract.ChangeItem) error {
		for _, ci := range input {
			if ci.Kind != abstract.InsertKind {
				continue
			}
			targetRow := ci.AsMap()
			keyRaw, ok := targetRow["t_int8"]
			if !ok {
				require.Fail(t, "faulty test: missing key column")
			}
			key, ok := keyRaw.(int32)
			if !ok {
				require.Fail(t, "key column is of wrong type", "wrong type %T", keyRaw)
			}
			checkDataRow(t, targetRow, TestData[key])
			totalInserts += 1
		}
		return nil
	}))

	require.Equal(t, len(TestData), totalInserts)
}
