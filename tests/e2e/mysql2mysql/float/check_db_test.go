package light

import (
	_ "embed"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/mysql/mysqlrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	Source = helpers.RecipeMysqlSource()
	Target = helpers.RecipeMysqlTarget(mysqlrecipe.WithPrefix("TARGET_"))

	//go:embed increment.sql
	IncrementStatements string
)

func init() {
	helpers.InitSrcDst(helpers.TransferID, Source, Target, abstract.TransferTypeSnapshotAndIncrement) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestFloat(t *testing.T) {
	transfer := helpers.MakeTransfer(helpers.TransferID, Source, Target, abstract.TransferTypeSnapshotAndIncrement)
	worker := helpers.Activate(t, transfer, nil)
	defer worker.Close(t)

	helpers.ExecuteMySQLStatementsLineByLine(t, IncrementStatements, helpers.NewMySQLConnectionParams(t, Source.ToStorageParams()))

	srcStorage, dstStorage := helpers.NewMySQLStorageFromSource(t, Source), helpers.NewMySQLStorageFromTarget(t, Target)
	require.NoError(t, helpers.WaitEqualRowsCountDifferentSchemas(t, Source.Database, Target.Database, "test", srcStorage, dstStorage, 30*time.Second))
	dumpSrc := helpers.MySQLDump(t, Source.ToStorageParams())
	dumpDst := helpers.MySQLDump(t, Target.ToStorageParams())
	canon.SaveJSON(t, map[string]interface{}{"src": dumpSrc, "dst": dumpDst})
}
