package provider

import (
	"testing"

	"github.com/stretchr/testify/require"
	yt_table "github.com/transferia/transferia/pkg/providers/yt/provider/table"
	yt_provider_types "github.com/transferia/transferia/pkg/providers/yt/provider/types"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/skiff"
)

func testYtColumn(t *testing.T, name string, ytType ytschema.ComplexType, nullable bool) yt_table.YtColumn {
	t.Helper()
	resolvedType, err := yt_provider_types.Resolve(ytType)
	require.NoError(t, err)
	return yt_table.NewColumn(
		name,
		resolvedType,
		ytType,
		ytschema.Column{Name: name, ComplexType: ytType, Required: !nullable},
		nullable,
	)
}

func testTableWithComplexVariantList(t *testing.T) yt_table.YtTable {
	t.Helper()
	tbl := yt_table.NewTable("types_test")
	tbl.AddColumn(testYtColumn(t, "id", ytschema.TypeUint8, false))
	tbl.AddColumn(testYtColumn(t, "complex_list", ytschema.List{
		Item: ytschema.Variant{
			Elements: []ytschema.TupleElement{
				{Type: ytschema.TypeInt64},
				{Type: ytschema.TypeString},
			},
		},
	}, false))
	return tbl
}

func TestBuildSkiffFormat_ComplexTypesKeepComplexWireTypes(t *testing.T) {
	tbl := testTableWithComplexVariantList(t)

	f := buildSkiffFormat(tbl, "")
	s, err := skiff.SingleSchema(f)
	require.NoError(t, err)
	require.Len(t, s.Children, 2)
	require.Equal(t, skiff.TypeUint8, s.Children[0].Type)
	require.Equal(t, skiff.TypeRepeatedVariant8, s.Children[1].Type)
}

func TestMakeMapRowConverter_ConvertsComplexVariantList(t *testing.T) {
	tbl := testTableWithComplexVariantList(t)
	decoder := newRowDecoder(tbl, "")
	require.True(t, decoder.useMapDecode)

	expectedComplex := []any{
		[]any{int64(0), int64(42)},
		[]any{int64(1), "hello"},
	}
	values, err := mapRowToValues(map[string]any{
		"id":           uint64(7),
		"complex_list": expectedComplex,
	}, 0, decoder.cols, decoder.idxColName)
	require.NoError(t, err)
	require.Len(t, values, 2)
	require.Equal(t, uint8(7), values[0])
	require.Equal(t, expectedComplex, values[1])
}

func TestBuildSkiffFormat_PreservesPrimitiveAndNullableSemantics(t *testing.T) {
	tbl := yt_table.NewTable("types_test")
	tbl.AddColumn(testYtColumn(t, "id", ytschema.TypeUint8, false))
	tbl.AddColumn(testYtColumn(t, "n_int64", ytschema.TypeInt64, true))
	tbl.AddColumn(testYtColumn(t, "complex_nullable", ytschema.List{Item: ytschema.TypeInt64}, true))
	decoder := newRowDecoder(tbl, "")
	require.True(t, decoder.useMapDecode)

	f := buildSkiffFormat(tbl, "")
	s, err := skiff.SingleSchema(f)
	require.NoError(t, err)
	require.Len(t, s.Children, 3)

	require.Equal(t, skiff.TypeUint8, s.Children[0].Type)
	require.Equal(t, skiff.TypeVariant8, s.Children[1].Type)
	require.Equal(t, skiff.TypeInt64, s.Children[1].Children[1].Type)
	require.Equal(t, skiff.TypeVariant8, s.Children[2].Type)
	require.Equal(t, skiff.TypeRepeatedVariant8, s.Children[2].Children[1].Type)

	values, err := mapRowToValues(map[string]any{
		"id":               uint64(3),
		"n_int64":          nil,
		"complex_nullable": nil,
	}, 0, decoder.cols, decoder.idxColName)
	require.NoError(t, err)
	require.Equal(t, []any{uint8(3), nil, nil}, values)
}
