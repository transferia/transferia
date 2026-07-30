package helpers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/test/canon"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

func RecipeYtTarget(path string) provider_yt.YtDestinationModel {
	ytDestination := provider_yt.NewYtDestinationV1(provider_yt.YtDestination{
		Cluster:       os.Getenv("YT_PROXY"),
		CellBundle:    "default",
		PrimaryMedium: "default",
		Path:          path,
	})
	ytDestination.WithDefaults()
	return ytDestination
}

func SetRecipeYt(dst *provider_yt.YtDestination) *provider_yt.YtDestination {
	dst.Cluster = os.Getenv("YT_PROXY")
	dst.CellBundle = "default"
	dst.PrimaryMedium = "default"
	return dst
}

type ReferenceTable struct {
	TableID     ypath.Path
	Rows        []any
	TableSchema ytschema.Schema
}

func DumpYtDirectoryToString(ytClient yt.Client, tablePath ypath.Path) (string, error) {
	var output strings.Builder

	var outNodes []struct {
		Name string `yson:",value"`
		Path string `yson:"path,attr"`
		Type string `yson:"type,attr"`
	}
	if err := ytClient.ListNode(context.Background(), ypath.Path(tablePath), &outNodes, &yt.ListNodeOptions{Attributes: []string{"type", "path"}}); err != nil {
		return "", xerrors.Errorf("list nodes error: %w", err)
	}

	for _, node := range outNodes {
		if node.Type != "table" {
			return "", xerrors.Errorf("no subdirectories allowed")
		}

		res := ReferenceTable{
			TableID: ypath.Path(node.Path),
		}
		if err := ytClient.GetNode(context.Background(), ypath.Path(fmt.Sprintf("%s/@schema", node.Path)), &res.TableSchema, nil); err != nil {
			return "", xerrors.Errorf("get schema: %w", err)
		}
		res.TableSchema.Strict = nil

		reader, err := ytClient.ReadTable(context.Background(), res.TableID, nil)
		if err != nil {
			return "", xerrors.Errorf("select rows: %w", err)
		}
		for reader.Next() {
			var value interface{}
			if err := reader.Scan(&value); err != nil {
				return "", xerrors.Errorf("scan item: %w", err)
			}
			res.Rows = append(res.Rows, value)
		}
		if reader.Err() != nil {
			return "", xerrors.Errorf("read: %w", err)
		}

		output.WriteString(fmt.Sprintf("%#v", res))

	}
	return output.String(), nil
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

func YtTypesTestData() ([]ytschema.Column, []map[string]any) {
	members := []ytschema.StructMember{
		{Name: "fieldInt16", Type: ytschema.TypeInt16},
		{Name: "fieldFloat32", Type: ytschema.TypeFloat32},
		{Name: "fieldString", Type: ytschema.TypeString},
	}
	elements := []ytschema.TupleElement{
		{Type: ytschema.TypeInt16},
		{Type: ytschema.TypeFloat32},
		{Type: ytschema.TypeString},
	}

	listSchema := ytschema.List{Item: ytschema.TypeFloat64}
	structSchema := ytschema.Struct{Members: members}
	tupleSchema := ytschema.Tuple{Elements: elements}
	namedVariantSchema := ytschema.Variant{Members: members}
	unnamedVariantSchema := ytschema.Variant{Elements: elements}
	dictSchema := ytschema.Dict{Key: ytschema.TypeString, Value: ytschema.TypeInt64}
	taggedSchema := ytschema.Tagged{Tag: "mytag", Item: ytschema.Tagged{Tag: "innerTag", Item: ytschema.TypeInt32}}

	schema := []ytschema.Column{
		{Name: "id", ComplexType: ytschema.TypeUint8, SortOrder: ytschema.SortAscending},
		{Name: "date_str", ComplexType: ytschema.TypeBytes},
		{Name: "datetime_str", ComplexType: ytschema.TypeBytes},
		{Name: "datetime_str2", ComplexType: ytschema.TypeBytes},
		{Name: "datetime_ts", ComplexType: ytschema.TypeInt64},
		{Name: "datetime_ts2", ComplexType: ytschema.TypeInt64},
		{Name: "intlist", ComplexType: ytschema.Optional{Item: ytschema.TypeAny}},
		{Name: "num_to_str", ComplexType: ytschema.TypeInt32},
		{Name: "decimal_as_float", ComplexType: ytschema.TypeFloat64},
		{Name: "decimal_as_string", ComplexType: ytschema.TypeString},
		{Name: "decimal_as_bytes", ComplexType: ytschema.TypeBytes},
		{Name: "null_value", ComplexType: ytschema.TypeNull},

		// Composite types below.
		{Name: "list", ComplexType: listSchema},
		{Name: "struct", ComplexType: structSchema},
		{Name: "tuple", ComplexType: tupleSchema},
		{Name: "variant_named", ComplexType: namedVariantSchema},
		{Name: "variant_unnamed", ComplexType: unnamedVariantSchema},
		{Name: "dict", ComplexType: dictSchema},
		{Name: "tagged", ComplexType: ytschema.Tagged{Tag: "mytag", Item: ytschema.Variant{Members: members}}},

		// That test mostly here for YtDictTransformer.
		// Iteration and transformation over all fields/elements/members of all complex types is tested by it.
		{Name: "nested1", ComplexType: ytschema.Struct{Members: []ytschema.StructMember{
			{Name: "list", Type: ytschema.List{
				Item: ytschema.Tuple{Elements: []ytschema.TupleElement{{Type: dictSchema}, {Type: dictSchema}}}},
			},
			{Name: "named", Type: ytschema.Variant{
				Members: []ytschema.StructMember{{Name: "d1", Type: dictSchema}, {Name: "d2", Type: dictSchema}},
			}},
		}}},

		// Use two different structs to prevent extracting long line to different file from result.json.
		{Name: "nested2", ComplexType: ytschema.Struct{Members: []ytschema.StructMember{
			{Name: "unnamed", Type: ytschema.Variant{
				Elements: []ytschema.TupleElement{{Type: dictSchema}, {Type: dictSchema}},
			}},
			{Name: "dict", Type: ytschema.Dict{Key: taggedSchema, Value: dictSchema}},
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
		"null_value":        nil,

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
		null_value Nullable(String),

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
	ytEnv, cancel := yttest.NewEnv(t)
	t.Cleanup(cancel)

	_, err := ytEnv.YT.CreateNode(ytEnv.Ctx, ypath.Path(path), yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)

	t.Cleanup(func() {
		err := ytEnv.YT.RemoveNode(ytEnv.Ctx, ypath.Path(path), &yt.RemoveNodeOptions{Recursive: true})
		require.NoError(t, err)
	})
	return ytEnv
}
