//go:build !disable_clickhouse_provider

package clickhouse

import (
	_ "embed"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
)

//go:embed typesystem.md
var canonDoc string

func TestTypeSystem(t *testing.T) {
	rules := typesystem.RuleFor(ProviderType)
	require.NotNil(t, rules.Source)
	require.NotNil(t, rules.Target)
	doc := typesystem.Doc(ProviderType, "ClickHouse")
	fmt.Print(doc)
	require.Equal(t, canonDoc, doc)
}
