package heteroparentdata

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
	"github.com/transferia/transferia/tests/helpers/transfer"
)

const (
	schema      = "public"
	parentTable = "parent"
	childTable1 = "child_1"
	childTable2 = "child_2"

	parentRows = 2
	child1Rows = 3
	child2Rows = 4
	totalRows  = parentRows + child1Rows + child2Rows
)

var Source = *pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("dump"),
	pgrecipe.WithEdit(func(pg *provider_postgres.PgSource) { pg.UseFakePrimaryKey = true }),
)

func init() {
	_ = os.Setenv("YC", "1")
	Source.WithDefaults()
}

// snapshotRows runs a snapshot-only transfer and returns the received data rows grouped by table name.
func snapshotRows(t *testing.T, collapseInheritTables bool, includeObjects []string) map[string][]abstract.ChangeItem {
	rows := make(map[string][]abstract.ChangeItem)
	sinker := mocksink.NewMockSink(func(input []abstract.ChangeItem) error {
		for _, item := range input {
			if item.Table != "__consumer_keeper" && item.IsRowEvent() {
				rows[item.Table] = append(rows[item.Table], item)
			}
		}
		return nil
	})
	target := &model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinker },
		Cleanup:       model.Drop,
	}
	src := Source
	src.CollapseInheritTables = collapseInheritTables
	transfer := transferhelpers.MakeTransfer(transferhelpers.GenerateTransferID(t.Name()), &src, target, abstract.TransferTypeSnapshotOnly)
	transfer.DataObjects = &model.DataObjects{IncludeObjects: includeObjects}
	_ = delivery.Activate(t, transfer)
	return rows
}

// TestNoCollapseOnlyParent verifies that when only the parent table is included and CollapseInheritTables=false,
// only the parent data is transferred. Childrens rows are NOT transferred because child tables are not included.
func TestNoCollapseOnlyParent(t *testing.T) {
	rows := snapshotRows(t, false, []string{schema + "." + parentTable})
	require.Len(t, rows[parentTable], parentRows)
	require.Empty(t, rows[childTable1])
	require.Empty(t, rows[childTable2])
}

// TestNoCollapse verifies that when all tables are included and CollapseInheritTables=false,
// each table is transferred independently. The parent has only its 1 row, each child has its own number of rows.
func TestNoCollapse(t *testing.T) {
	rows := snapshotRows(t, false, []string{schema + "." + parentTable, schema + "." + childTable1, schema + "." + childTable2})
	require.Len(t, rows[parentTable], parentRows)
	require.Len(t, rows[childTable1], child1Rows)
	require.Len(t, rows[childTable2], child2Rows)
}

// TestCollapseOnlyParent verifies that when only the parent table is included and CollapseInheritTables=true,
// all rows are transferred as parent table.
func TestCollapseOnlyParent(t *testing.T) {
	rows := snapshotRows(t, true, []string{schema + "." + parentTable})
	require.Len(t, rows[parentTable], totalRows)
	require.Empty(t, rows[childTable1])
	require.Empty(t, rows[childTable2])
}

// TestNoCollapseOnlyChild verifies that when only a child table is included, parent direct rows are NOT included.
func TestNoCollapseOnlyChild(t *testing.T) {
	rows := snapshotRows(t, false, []string{schema + "." + childTable1})
	require.Empty(t, rows[parentTable])
	require.Len(t, rows[childTable1], child1Rows)
	require.Empty(t, rows[childTable2])
}
