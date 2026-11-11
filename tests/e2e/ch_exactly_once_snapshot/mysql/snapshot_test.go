package mysql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/providers/mysql/mysqlrecipe"
	eo "github.com/transferia/transferia/tests/e2e/ch_exactly_once_snapshot"
	_ "github.com/transferia/transferia/tests/helpers/registration/mysql"
)

func TestExactlyOnceSnapshot(test *testing.T) {
	eo.ForDatabases(test, func(test *testing.T) {
		for _, consistent := range []bool{false, true} {
			for _, workers := range []int{1, 2} {
				test.Run(fmt.Sprintf("consistent%t/workers%d", consistent, workers), func(test *testing.T) {
					source := mysqlrecipe.RecipeMysqlSource()
					source.ConsistentSnapshot = consistent
					source.SnapshotDegreeOfParallelism = 4
					source.IncludeTableRegex = []string{source.Database + "\\.(rows|empty)"}
					target := eo.NewTarget(test, "")
					eo.Run(test, source, target, workers)
					eo.CheckRows(test, target, "rows", 257)
					eo.CheckRows(test, target, "empty", 0)
					eo.CheckKeyPayload(test, target, "rows", 257)
					require.True(test, target.ExactlyOnceSnapshot)
				})
			}
		}
	})
}
