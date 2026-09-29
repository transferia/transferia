package pg15

import (
	"maps"
	"strings"
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

	// The PostgreSQL 15 recipe is built without libxml support.
	tableSQLs := maps.Clone(postgres_canon.TableSQLs)
	tableSQLs["public.wtf_types"] = strings.NewReplacer(
		"    t_xml xml,\n", "",
		"    '<root><value>iceberg</value></root>', -- t_xml\n", "",
		"    t_xml,\n", "",
		"    ''::xml,\n", "",
	).Replace(tableSQLs["public.wtf_types"])

	common.TestAllDataTypes(t, source, target, tableSQLs)
}
