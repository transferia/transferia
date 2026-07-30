package pg10

import (
	"testing"

	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/e2e/pg2pg/all_types/common"
)

func TestAllDataTypes(t *testing.T) {
	source := pgrecipe.RecipeSource(pgrecipe.WithPrefix(""))
	source.WithDefaults()
	target := pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))

	common.TestAllDataTypes(t, source, target)
}
