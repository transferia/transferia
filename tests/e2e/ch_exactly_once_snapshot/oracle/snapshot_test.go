package oracle

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	provider_oracle "github.com/transferia/transferia/pkg/providers/oracle"
	"github.com/transferia/transferia/pkg/providers/oracle/oraclerecipe"
	eo "github.com/transferia/transferia/tests/e2e/ch_exactly_once_snapshot"
	_ "github.com/transferia/transferia/tests/helpers/registration/oracle"
)

func TestExactlyOnceSnapshot(test *testing.T) {
	source := oraclerecipe.RecipeOracleSource()
	require.NoError(test, oraclerecipe.ExecSQL(context.Background(), source, `
 CREATE USER DT_EO IDENTIFIED BY test_eo_password;
 GRANT CONNECT, RESOURCE, UNLIMITED TABLESPACE TO DT_EO;
 CREATE TABLE DT_EO.eo_rows (id NUMBER(19,0) PRIMARY KEY, payload VARCHAR2(64));
 INSERT INTO DT_EO.eo_rows SELECT LEVEL, 'row-' || TO_CHAR(LEVEL) FROM dual CONNECT BY LEVEL <= 257;
 CREATE TABLE DT_EO.eo_empty (id NUMBER(19,0) PRIMARY KEY, payload VARCHAR2(64));
 COMMIT;
 `))
	source.IncludeTables = []string{"DT_EO.EO_ROWS", "DT_EO.EO_EMPTY"}
	source.RowIDBytesPerShard = 16 * 1024
	eo.ForDatabases(test, func(test *testing.T) {
		for _, consistent := range []bool{false, true} {
			for _, workers := range []int{1, 2} {
				test.Run(fmt.Sprintf("consistent%t/workers%d", consistent, workers), func(test *testing.T) {
					currSource := *source
					currSource.IsNonConsistentSnapshot = !consistent
					if consistent {
						// The non-consistent recipe disables the log tracker; flashback needs a saved SCN.
						currSource.TrackerType = provider_oracle.OracleInternalLogTracker
					}
					target := eo.NewTarget(test, "")
					eo.Run(test, &currSource, target, workers)
					eo.CheckRows(test, target, "eo_rows", 257)
					eo.CheckRows(test, target, "eo_empty", 0)
					eo.CheckKeyPayload(test, target, "eo_rows", 257)
				})
			}
		}
	})
}
