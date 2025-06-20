//go:build !disable_delta_provider

package delta

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
	doc := typesystem.Doc(ProviderType, "Delta Lake")
	fmt.Print(doc)
	require.Equal(t, canonDoc, doc)
}
