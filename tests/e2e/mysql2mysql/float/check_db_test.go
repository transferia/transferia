package light

import (
	_ "embed"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/library/go/test/canon"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/tests/helpers"
)

var (
	Source = helpers.RecipeMysqlSource()
	Target = helpers.RecipeMysqlTarget()

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
