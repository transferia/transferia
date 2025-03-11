package postgres

import (
	_ "embed"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/pkg/abstract/typesystem"
)

var (
	//go:embed typesystem.md
	canonDoc string
)

func TestTypeSystem(t *testing.T) {
	rules := typesystem.RuleFor(ProviderType)
	require.NotNil(t, rules.Source)
	require.NotNil(t, rules.Target)
	doc := typesystem.Doc(ProviderType, "PostgreSQL")
	fmt.Print(doc)
	require.Equal(t, canonDoc, doc)
}
