package pg10

import (
	"testing"

	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	postgres_canon "github.com/transferia/transferia/tests/canon/postgres"
	"github.com/transferia/transferia/tests/e2e/pg2pg/all_types/common"
	_ "github.com/transferia/transferia/tests/helpers/registration/postgres"
)

func TestAllDataTypes(t *testing.T) {
	source := pgrecipe.RecipeSource(pgrecipe.WithPrefix(""))
	source.WithDefaults()
	target := pgrecipe.RecipeTarget(pgrecipe.WithPrefix("DB0_"))

	common.TestAllDataTypes(t, source, target, postgres_canon.TableSQLs)
}
