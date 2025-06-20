//go:build !disable_mysql_provider

package mysql

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
	doc := typesystem.Doc(ProviderType, "MySQL")
	fmt.Print(doc)
	require.Equal(t, canonDoc, doc)
}
