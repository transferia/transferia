//go:build !disable_yt_provider

package recipe

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/test/canon"
	yt_provider "github.com/transferia/transferia/pkg/providers/yt"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

func TestContainerEnabled() bool {
	return os.Getenv("USE_TESTCONTAINERS") == "1"
}

func RecipeYtTarget(path string) (yt_provider.YtDestinationModel, func() error, error) {
	ytModel := new(yt_provider.YtDestination)
	ytModel.CellBundle = "default"
	ytModel.PrimaryMedium = "default"
	ytModel.Path = path
	cancel := func() error { return nil }

	if TestContainerEnabled() {
		container, err := RunContainer(context.Background(), testcontainers.WithImage("ytsaurus/local:stable"))
		if err != nil {
			return nil, cancel, xerrors.Errorf("run container: %w", err)
		}
		proxy, err := container.ConnectionHost(context.Background())
		if err != nil {
			return nil, cancel, xerrors.Errorf("connection host: %w", err)
		}
		ytModel.Cluster = proxy
		ytModel.Token = container.Token()

		ytDestination := yt_provider.NewYtDestinationV1(*ytModel)
		ytDestination.WithDefaults()
		cancel = func() error {
			return container.Terminate(context.Background())
		}
		return ytDestination, cancel, nil
	}
	ytModel.Cluster = os.Getenv("YT_PROXY")
	ytDestination := yt_provider.NewYtDestinationV1(*ytModel)
	ytDestination.WithDefaults()
	return ytDestination, cancel, nil
}

func SetRecipeYt(dst *yt_provider.YtDestination) *yt_provider.YtDestination {
	dst.Cluster = os.Getenv("YT_PROXY")
	dst.CellBundle = "default"
	dst.PrimaryMedium = "default"
	return dst
}

func DumpDynamicYtTable(ytClient yt.Client, tablePath ypath.Path, writer io.Writer) error {
	// Write schema
	schema := new(yson.RawValue)
	if err := ytClient.GetNode(context.Background(), ypath.Path(fmt.Sprintf("%s/@schema", tablePath)), schema, nil); err != nil {
		return xerrors.Errorf("get schema: %w", err)
	}
	if err := yson.NewEncoderWriter(yson.NewWriterConfig(writer, yson.WriterConfig{Format: yson.FormatPretty})).Encode(*schema); err != nil {
		return xerrors.Errorf("encode schema: %w", err)
	}
	if _, err := writer.Write([]byte{'\n'}); err != nil {
		return xerrors.Errorf("write: %w", err)
	}

	reader, err := ytClient.SelectRows(context.Background(), fmt.Sprintf("* from [%s]", tablePath), nil)
	if err != nil {
		return xerrors.Errorf("select rows: %w", err)
	}

	// Write data
	i := 0
	for reader.Next() {
		var value interface{}
		if err := reader.Scan(&value); err != nil {
			return xerrors.Errorf("scan item %d: %w", i, err)
		}
		if err := json.NewEncoder(writer).Encode(value); err != nil {
			return xerrors.Errorf("encode item %d: %w", i, err)
		}
		i++
	}
	if reader.Err() != nil {
		return xerrors.Errorf("read: %w", err)
	}
	return nil
}

func CanonizeDynamicYtTable(t *testing.T, ytClient yt.Client, tablePath ypath.Path, fileName string) {
	file, err := os.Create(fileName)
	require.NoError(t, err)
	require.NoError(t, DumpDynamicYtTable(ytClient, tablePath, file))
	require.NoError(t, file.Close())
	canon.SaveFile(t, fileName, canon.WithLocal(true))
}

func YtTestDir(t *testing.T, testSuiteName string) ypath.Path {
	return ypath.Path(fmt.Sprintf("//home/cdc/test/mysql2yt/%s/%s", testSuiteName, t.Name()))
}

func readAllRows[OutRow any](t *testing.T, ytEnv *yttest.Env, path ypath.Path) []OutRow {
	reader, err := ytEnv.YT.SelectRows(
		context.Background(),
		fmt.Sprintf("* from [%s]", path),
		nil,
	)
	require.NoError(t, err)

	outRows := make([]OutRow, 0)

	for reader.Next() {
		var row OutRow
		require.NoError(t, reader.Scan(&row), "Error reading row")
		outRows = append(outRows, row)
	}

	require.NoError(t, reader.Close())
	return outRows
}

func YtReadAllRowsFromAllTables[OutRow any](t *testing.T, cluster string, path string, expectedResCount int) []OutRow {
	ytEnv := yttest.New(t, yttest.WithConfig(yt.Config{Proxy: cluster}), yttest.WithLogger(logger.Log.Structured()))
	ytPath, err := ypath.Parse(path)
	require.NoError(t, err)

	exists, err := ytEnv.YT.NodeExists(context.Background(), ytPath.Path, nil)
	require.NoError(t, err)
	if !exists {
		return []OutRow{}
	}

	var tables []struct {
		Name string `yson:",value"`
	}

	require.NoError(t, ytEnv.YT.ListNode(context.Background(), ytPath, &tables, nil))

	resRows := make([]OutRow, 0, expectedResCount)
	for _, tableDesc := range tables {
		subPath := ytPath.Copy().Child(tableDesc.Name)
		readed := readAllRows[OutRow](t, ytEnv, subPath.Path)
		resRows = append(resRows, readed...)
	}
	return resRows
}

func YtTypesTestData() ([]schema.Column, []map[string]any) {
	members := []schema.StructMember{
		{Name: "fieldInt16", Type: schema.TypeInt16},
		{Name: "fieldFloat32", Type: schema.TypeFloat32},
		{Name: "fieldString", Type: schema.TypeString},
	}
	elements := []schema.TupleElement{
		{Type: schema.TypeInt16},
		{Type: schema.TypeFloat32},
		{Type: schema.TypeString},
	}

	listSchema := schema.List{Item: schema.TypeFloat64}
	structSchema := schema.Struct{Members: members}
	tupleSchema := schema.Tuple{Elements: elements}
	namedVariantSchema := schema.Variant{Members: members}
	unnamedVariantSchema := schema.Variant{Elements: elements}
	dictSchema := schema.Dict{Key: schema.TypeString, Value: schema.TypeInt64}
	taggedSchema := schema.Tagged{Tag: "mytag", Item: schema.Tagged{Tag: "innerTag", Item: schema.TypeInt32}}

	schema := []schema.Column{
		{Name: "id", ComplexType: schema.TypeUint8, SortOrder: schema.SortAscending},
		{Name: "date_str", ComplexType: schema.TypeBytes},
		{Name: "datetime_str", ComplexType: schema.TypeBytes},
		{Name: "datetime_str2", ComplexType: schema.TypeBytes},
		{Name: "datetime_ts", ComplexType: schema.TypeInt64},
		{Name: "datetime_ts2", ComplexType: schema.TypeInt64},
		{Name: "intlist", ComplexType: schema.Optional{Item: schema.TypeAny}},
		{Name: "num_to_str", ComplexType: schema.TypeInt32},
		{Name: "decimal_as_float", ComplexType: schema.TypeFloat64},
		{Name: "decimal_as_string", ComplexType: schema.TypeString},
		{Name: "decimal_as_bytes", ComplexType: schema.TypeBytes},

		// Composite types below.
		{Name: "list", ComplexType: listSchema},
		{Name: "struct", ComplexType: structSchema},
		{Name: "tuple", ComplexType: tupleSchema},
		{Name: "variant_named", ComplexType: namedVariantSchema},
		{Name: "variant_unnamed", ComplexType: unnamedVariantSchema},
		{Name: "dict", ComplexType: dictSchema},
		{Name: "tagged", ComplexType: schema.Tagged{Tag: "mytag", Item: schema.Variant{Members: members}}},

		// That test mostly here for YtDictTransformer.
		// Iteration and transformation over all fields/elements/members of all complex types is tested by it.
		{Name: "nested1", ComplexType: schema.Struct{Members: []schema.StructMember{
			{
				Name: "list", Type: schema.List{
					Item: schema.Tuple{Elements: []schema.TupleElement{{Type: dictSchema}, {Type: dictSchema}}},
				},
			},
			{Name: "named", Type: schema.Variant{
				Members: []schema.StructMember{{Name: "d1", Type: dictSchema}, {Name: "d2", Type: dictSchema}},
			}},
		}}},

		// Use two different structs to prevent extracting long line to different file from result.json.
		{Name: "nested2", ComplexType: schema.Struct{Members: []schema.StructMember{
			{Name: "unnamed", Type: schema.Variant{
				Elements: []schema.TupleElement{{Type: dictSchema}, {Type: dictSchema}},
			}},
			{Name: "dict", Type: schema.Dict{Key: taggedSchema, Value: dictSchema}},
		}}},
	}

	listData := []float64{-1.01, 2.0, 1294.21}
	structData := map[string]any{"fieldInt16": 100, "fieldFloat32": 100.01, "fieldString": "abc"}
	tupleData := []any{-5, 300.03, "my data"}
	namedVariantData := []any{"fieldString", "magotan"}
	unnamedVariantData := []any{1, 300.03}
	dictData := [][]any{{"k1", 1}, {"k2", 2}, {"k3", 3}}

	data := []map[string]any{{
		"id":                uint8(1),
		"date_str":          "2022-03-10",
		"datetime_str":      "2022-03-10T01:02:03",
		"datetime_str2":     "2022-03-10 01:02:03",
		"datetime_ts":       int64(0),
		"datetime_ts2":      int64(1646940559),
		"intlist":           []int64{1, 2, 3},
		"num_to_str":        int32(100),
		"decimal_as_float":  2.3456,
		"decimal_as_string": "23.45",
		"decimal_as_bytes":  []byte("67.89"),

		"list":            listData,
		"struct":          structData,
		"tuple":           tupleData,
		"variant_named":   namedVariantData,
		"variant_unnamed": unnamedVariantData,
		"dict":            dictData,
		"tagged":          []any{"fieldInt16", 100},

		"nested1": map[string]any{
			"list":  []any{[]any{dictData, dictData}},
			"named": []any{"d2", dictData},
		},

		"nested2": map[string]any{
			"unnamed": []any{1, dictData},
			"dict":    [][]any{{10, dictData}, {11, dictData}},
		},
	}}

	return schema, data
}

func ChSchemaForYtTypesTestData() string {
	return `
		id UInt8,
		date_str Date,
		datetime_str DateTime,
		datetime_str2 DateTime,
		datetime_ts DateTime,
		datetime_ts2 DateTime,
		intlist Array(Int64),
		num_to_str String,
		decimal_as_float Decimal(10, 7),
		decimal_as_string Decimal(10, 7),
		decimal_as_bytes Decimal(10, 7),

		struct String,
		list String,
		tuple String,
		variant_named String,
		variant_unnamed String,
		dict String,
		tagged String,

		nested1 String,
		nested2 String
	`
}

func NewEnvWithNode(t *testing.T, path string) *yttest.Env {
	ytEnv, cancel := NewEnv(t)
	t.Cleanup(cancel)

	_, err := ytEnv.YT.CreateNode(ytEnv.Ctx, ypath.Path(path), yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)

	t.Cleanup(func() {
		err := ytEnv.YT.RemoveNode(ytEnv.Ctx, ypath.Path(path), &yt.RemoveNodeOptions{Recursive: true})
		require.NoError(t, err)
	})
	return ytEnv
}
