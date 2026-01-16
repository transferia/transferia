package clickhouse

import (
	_ "embed"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"go.ytsaurus.tech/yt/go/schema"
)

var (
	//go:embed typesystem.md
	canonDoc string
)

func TestTypeSystem(t *testing.T) {
	rules := typesystem.RuleFor(ProviderType)
	require.NotNil(t, rules.Source)
	require.NotNil(t, rules.Target)
	doc := typesystem.Doc(ProviderType, "ClickHouse")
	fmt.Print(doc)
	require.Equal(t, canonDoc, doc)
}

func TestIsAlterPossible(t *testing.T) {
	makeCol := func(originalType, dataType string, required bool) abstract.ColSchema {
		return abstract.ColSchema{
			ColumnName:   "col",
			OriginalType: originalType,
			DataType:     dataType,
			Required:     required,
		}
	}

	t.Run("reject nullable change", func(t *testing.T) {
		require.Error(t, isAlterPossible(
			makeCol("Int8", schema.TypeInt8.String(), true),
			makeCol("Int8", schema.TypeInt8.String(), false),
		))
	})

	t.Run("reject same type", func(t *testing.T) {
		require.Error(t, isAlterPossible(
			makeCol("Int8", schema.TypeInt8.String(), true),
			makeCol("Int8", schema.TypeInt8.String(), true),
		))
	})

	t.Run("reject disallowed type change", func(t *testing.T) {
		require.Error(t, isAlterPossible(
			makeCol("Int8", schema.TypeInt8.String(), true),
			makeCol("String", schema.TypeString.String(), true),
		))
	})

	t.Run("allow widening integer type", func(t *testing.T) {
		require.NoError(t, isAlterPossible(
			makeCol("Int8", schema.TypeInt8.String(), true),
			makeCol("Int16", schema.TypeInt16.String(), true),
		))
	})

	t.Run("disallow limiting integer type", func(t *testing.T) {
		require.Error(t, isAlterPossible(
			makeCol("Int16", schema.TypeInt16.String(), true),
			makeCol("Int8", schema.TypeInt8.String(), true),
		))
	})
}
