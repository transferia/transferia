//go:build !disable_airbyte_provider

package airbyte

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
	require.Nil(t, rules.Target)
	doc := typesystem.Doc(ProviderType, "Airbyte")
	fmt.Print(doc)
	require.Equal(t, canonDoc, doc)
}
