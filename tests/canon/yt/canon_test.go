package yt

import (
	"context"
	"math"
	"os"
	"testing"
	"time"

	"github.com/spf13/cast"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/yt_client"
	"github.com/transferia/transferia/tests/canon/validator"
	"github.com/transferia/transferia/tests/helpers"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
}

var TestData = []map[string]any{
	{
		"t_int8":      math.MinInt8,
		"t_int16":     math.MinInt16,
		"t_int32":     math.MinInt32,
		"t_int64":     math.MinInt64,
		"t_uint8":     0,
		"t_uint16":    0,
		"t_uint32":    0,
		"t_uint64":    0,
		"t_float":     float32(0.0),
		"t_double":    0.0,
		"t_bool":      false,
		"t_string":    "",
		"t_utf8":      "",
		"t_date":      0,                                      // Min allowed by YT Date.
		"t_datetime":  0,                                      // Min allowed by YT Datetime.
		"t_timestamp": 0,                                      // Min allowed by YT Timestamp.
		"t_interval":  ytInterval(-49673*24*time.Hour + 1000), // Min allowed by YT Duration.
		// "t_yson":       It is optional field and not enabled here.
		// "t_opt_int64":  It is optional field and not enabled here.
		"t_list":            []float64{},
		"t_struct":          map[string]any{"fieldInt16": 100, "fieldFloat32": 100.01, "fieldString": "abc"},
		"t_tuple":           []any{-5, 300.03, "my data"},
		"t_variant_named":   []any{"fieldInt16", 100},
		"t_variant_unnamed": []any{0, 100},
		"t_dict":            [][]any{},
		"t_tagged":          []any{"fieldInt16", 100},
	},
	{
		"t_int8":            10,
		"t_int16":           -2000,
		"t_int32":           -200000,
		"t_int64":           -20000000000,
		"t_uint8":           20,
		"t_uint16":          2000,
		"t_uint32":          2000000,
		"t_uint64":          20000000000,
		"t_float":           float32(2.2),
		"t_double":          2.2,
		"t_bool":            true,
		"t_string":          "Test byte string 2",
		"t_utf8":            "Test utf8 string 2",
		"t_date":            1640604030 / secondsPerDay,
		"t_datetime":        1640604030,
		"t_timestamp":       1640604030502383,
		"t_interval":        ytInterval(time.Minute),
		"t_yson":            []uint64{100, 200, 300},
		"t_opt_int64":       math.MaxInt64,
		"t_list":            []float64{-1.01},
		"t_struct":          map[string]any{"fieldInt16": 100, "fieldFloat32": 100.01, "fieldString": "abc"},
		"t_tuple":           []any{-5, 300.03, "my data"},
		"t_variant_named":   []any{"fieldFloat32", 100.01},
		"t_variant_unnamed": []any{1, 100.01},
		"t_dict":            [][]any{{"my_key", 100}},
		"t_tagged":          []any{"fieldFloat32", 100.01},
	},
	{
		"t_int8":            math.MaxInt8,
		"t_int16":           math.MaxInt16,
		"t_int32":           math.MaxInt32,
		"t_int64":           math.MaxInt64,
		"t_uint8":           math.MaxUint8,
		"t_uint16":          math.MaxInt16, // TODO: Replace to math.MaxUint16 while fixing TM-7588.
		"t_uint32":          math.MaxInt32, // TODO: Replace to math.MaxUint32 while fixing TM-7588.
		"t_uint64":          math.MaxInt64, // TODO: Replace to math.MaxUint32 while fixing TM-7588.
		"t_float":           float32(42),
		"t_double":          42.0,
		"t_bool":            false,
		"t_string":          "Test byte string 3",
		"t_utf8":            "Test utf8 string 3",
		"t_date":            cast.ToTime("2105-12-31T23:59:59").Unix() / secondsPerDay, // Max allowed by YT Date.
		"t_datetime":        cast.ToTime("2105-12-31T23:59:59").Unix(),                 // Max allowed by YT Datetime.
		"t_timestamp":       cast.ToTime("2105-12-31 23:59:59").UnixMicro(),            // TODO: Max allowed by CH-target Timestamp.
		"t_interval":        ytInterval(49673*24*time.Hour - 1000),                     // Max allowed by YT Duration.
		"t_yson":            nil,
		"t_opt_int64":       nil,
		"t_list":            []float64{-1.01, 2.0, 1294.21},
		"t_struct":          map[string]any{"fieldInt16": 100, "fieldFloat32": 100.01, "fieldString": "abc"},
		"t_tuple":           []any{-5, 300.03, "my data"},
		"t_variant_named":   []any{"fieldString", "magotan"},
		"t_variant_unnamed": []any{2, "magotan"},
		"t_dict":            [][]any{{"key1", 1}, {"key2", 20}, {"key3", 300}},
		"t_tagged":          []any{"fieldString", "100"},
	},
}

func ytInterval(duration time.Duration) ytschema.Interval {
	res, err := ytschema.NewInterval(duration)
	if err != nil {
		panic(err)
	}
	return res
}

var (
	members = []ytschema.StructMember{
		{Name: "fieldInt16", Type: ytschema.TypeInt16},
		{Name: "fieldFloat32", Type: ytschema.TypeFloat32},
		{Name: "fieldString", Type: ytschema.TypeString},
	}
	elements = []ytschema.TupleElement{
		{Type: ytschema.TypeInt16},
		{Type: ytschema.TypeFloat32},
		{Type: ytschema.TypeString},
	}
	secondsPerDay = int64(24 * 60 * 60)
)

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
	{Name: "t_interval", ComplexType: ytschema.TypeInterval}, // FIXME: support in CH
	{Name: "t_yson", ComplexType: ytschema.Optional{Item: ytschema.TypeAny}},
	{Name: "t_opt_int64", ComplexType: ytschema.Optional{Item: ytschema.TypeInt64}},
	{Name: "t_list", ComplexType: ytschema.List{Item: ytschema.TypeFloat64}},
	{Name: "t_struct", ComplexType: ytschema.Struct{Members: members}},
	{Name: "t_tuple", ComplexType: ytschema.Tuple{Elements: elements}},
	{Name: "t_variant_named", ComplexType: ytschema.Variant{Members: members}},
	{Name: "t_variant_unnamed", ComplexType: ytschema.Variant{Elements: elements}},
	{Name: "t_dict", ComplexType: ytschema.Dict{Key: ytschema.TypeString, Value: ytschema.TypeInt64}},
	{Name: "t_tagged", ComplexType: ytschema.Tagged{Tag: "mytag", Item: ytschema.Variant{Members: members}}},
}

func TestCanonSource(t *testing.T) {
	Source := &provider_yt.YtSource{
		Cluster:          os.Getenv("YT_PROXY"),
		YtProxy:          os.Getenv("YT_PROXY"),
		Paths:            []string{"//home/cdc/junk/test_table"},
		YtToken:          "",
		RowIdxColumnName: "row_idx",
	}
	Source.WithDefaults()

	createTestData(t, Source, Source.Paths[0])

	transfer := helpers.MakeTransfer(
		helpers.TransferID,
		Source,
		&model.MockDestination{
			SinkerFactory: validator.New(model.IsStrictSource(Source), validator.CanonizatorSkipEmptyClose(t)),
			Cleanup:       model.DisabledCleanup,
		},
		abstract.TransferTypeSnapshotOnly,
	)
	_ = helpers.Activate(t, transfer)
}

func TestCanonSourceWithDataObjects(t *testing.T) {
	Source := &provider_yt.YtSource{
		Cluster:          os.Getenv("YT_PROXY"),
		YtProxy:          os.Getenv("YT_PROXY"),
		Paths:            []string{"//home/cdc/junk/test_parent_dir"},
		YtToken:          "",
		RowIdxColumnName: "row_idx",
	}
	Source.WithDefaults()

	createTestData(t, Source, "//home/cdc/junk/test_parent_dir/nested_dir/some_table")

	transfer := helpers.MakeTransfer(
		helpers.TransferID,
		Source,
		&model.MockDestination{
			SinkerFactory: validator.New(model.IsStrictSource(Source), validator.CanonizatorSkipEmptyClose(t)),
			Cleanup:       model.DisabledCleanup,
		},
		abstract.TransferTypeSnapshotOnly,
	)
	transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{"//home/cdc/junk/test_parent_dir/nested_dir/some_table"}}
	_ = helpers.Activate(t, transfer)
}

func TestCanonSourceWithDirInDataObjects(t *testing.T) {
	Source := &provider_yt.YtSource{
		Cluster:          os.Getenv("YT_PROXY"),
		YtProxy:          os.Getenv("YT_PROXY"),
		Paths:            []string{"//home/cdc/junk/test_parent_dir"},
		YtToken:          "",
		RowIdxColumnName: "row_idx",
	}
	Source.WithDefaults()

	createTestData(t, Source, "//home/cdc/junk/test_parent_dir/nested_dir2/nested_dir3/some_table2")

	transfer := helpers.MakeTransfer(
		helpers.TransferID,
		Source,
		&model.MockDestination{
			SinkerFactory: validator.New(model.IsStrictSource(Source), validator.CanonizatorSkipEmptyClose(t)),
			Cleanup:       model.DisabledCleanup,
		},
		abstract.TransferTypeSnapshotOnly,
	)
	transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{"//home/cdc/junk/test_parent_dir/nested_dir2"}}
	_ = helpers.Activate(t, transfer)
}

func createTestData(t *testing.T, Source *provider_yt.YtSource, path string) {
	ytc, err := yt_client.NewYtClientWrapper(yt_client.HTTP, nil, &yt.Config{Proxy: Source.YtProxy})
	require.NoError(t, err)
	_ = ytc.RemoveNode(context.Background(), ypath.NewRich(path).YPath(), nil)

	sch := ytschema.Schema{
		Strict:     nil,
		UniqueKeys: false,
		Columns:    YtColumns,
	}

	ctx := context.Background()
	wr, err := yt.WriteTable(ctx, ytc, ypath.NewRich(path).YPath(), yt.WithCreateOptions(yt.WithSchema(sch), yt.WithRecursive()))
	require.NoError(t, err)
	// var optint int64 = 10050
	for _, row := range TestData {
		require.NoError(t, wr.Write(row))
	}
	require.NoError(t, wr.Commit())
}
