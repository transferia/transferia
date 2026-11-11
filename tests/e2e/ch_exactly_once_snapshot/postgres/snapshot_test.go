package postgres

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	eo "github.com/transferia/transferia/tests/e2e/ch_exactly_once_snapshot"
	_ "github.com/transferia/transferia/tests/helpers/registration/postgres"
)

func TestExactlyOnceSnapshot(test *testing.T) {
	eo.ForDatabases(test, func(test *testing.T) {
		for _, serialization := range []provider_postgres.PgSerializationFormat{"text", "binary"} {
			for _, workers := range []int{1, 2} {
				test.Run(fmt.Sprintf("%s/workers%d", serialization, workers), func(test *testing.T) {
					source := pgrecipe.RecipeSource()
					source.DBTables = []string{"public.rows", "public.empty"}
					source.SnapshotSerializationFormat = serialization
					source.DesiredTableSize = 1
					source.SnapshotDegreeOfParallelism = 4
					target := eo.NewTarget(test, "")
					eo.Run(test, source, target, workers)
					eo.CheckRows(test, target, "rows", 4096)
					eo.CheckRows(test, target, "empty", 0)
					eo.CheckKeyPayload(test, target, "rows", 4096)
					connection := eo.OpenTarget(test, target)
					var mismatchedNullableValues uint64
					require.NoError(test, connection.QueryRow("SELECT count() FROM `"+target.Database+"`.rows WHERE if(id % 7 = 0, isNotNull(optional_value), isNull(optional_value) OR optional_value != id % 101)").Scan(&mismatchedNullableValues))
					require.Zero(test, mismatchedNullableValues)
				})
			}
		}
	})
}
