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
	primType, err := yt_provider_types.Resolve(ytType)
	require.NoError(t, err)
	return yt_table.NewColumn(
		name,
		primType,
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

func TestNullTypedColumn_ExcludedFromWireAndDecodedAsNil(t *testing.T) {
	tbl := yt_table.NewTable("types_test")
	tbl.AddColumn(testYtColumn(t, "id", ytschema.TypeUint8, false))
	tbl.AddColumn(testYtColumn(t, "n", ytschema.TypeNull, false))
	tbl.AddColumn(testYtColumn(t, "name", ytschema.TypeString, false))

	f := buildSkiffFormat(tbl, "")
	s, err := skiff.SingleSchema(f)
	require.NoError(t, err)
	require.Len(t, s.Children, 2)
	require.Equal(t, "id", s.Children[0].Name)
	require.Equal(t, "name", s.Children[1].Name)

	decoder := newRowDecoder(tbl, "")
	require.False(t, decoder.useMapDecode)
	require.Equal(t, 2, decoder.rowType.NumField())

	rd := decoder.cloneForReader()
	row := rd.rowPtr.Elem()
	row.Field(0).SetUint(5)
	row.Field(1).SetString("abc")
	values, err := rd.arenaConv.Convert(row, 0)
	require.NoError(t, err)
	require.Equal(t, []any{uint8(5), nil, "abc"}, values)
}

func TestNullTypedColumn_MapDecodePathYieldsNil(t *testing.T) {
	tbl := yt_table.NewTable("types_test")
	tbl.AddColumn(testYtColumn(t, "id", ytschema.TypeUint8, false))
	tbl.AddColumn(testYtColumn(t, "n", ytschema.TypeNull, false))
	tbl.AddColumn(testYtColumn(t, "complex_list", ytschema.List{Item: ytschema.TypeInt64}, false))

	decoder := newRowDecoder(tbl, "")
	require.True(t, decoder.useMapDecode)

	// The null-typed column is absent from the Skiff format, so the decoded map has no key for it.
	values, err := mapRowToValues(map[string]any{
		"id":           uint64(1),
		"complex_list": []any{int64(9)},
	}, 0, decoder.cols, decoder.idxColName)
	require.NoError(t, err)
	require.Equal(t, []any{uint8(1), nil, []any{int64(9)}}, values)
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
