package light

import (
	_ "embed"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/test/canon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/mysql/mysqlrecipe"
	"github.com/transferia/transferia/tests/helpers/delivery"
	"github.com/transferia/transferia/tests/helpers/mysql"
	_ "github.com/transferia/transferia/tests/helpers/registration/mysql"
	"github.com/transferia/transferia/tests/helpers/storage"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
)

var (
	Source = mysql.RecipeMysqlSource()
	Target = mysql.RecipeMysqlTarget(mysqlrecipe.WithPrefix("TARGET_"))

	//go:embed increment.sql
	IncrementStatements string
)

func init() {
	transferhelpers.InitSrcDst(transferhelpers.TransferID, Source, Target, abstract.TransferTypeSnapshotAndIncrement) // to WithDefaults() & FillDependentFields(): IsHomo, helpers.TransferID, IsUpdateable
}

func TestFloat(t *testing.T) {
	transfer := transferhelpers.MakeTransfer(transferhelpers.TransferID, Source, Target, abstract.TransferTypeSnapshotAndIncrement)
	worker := delivery.Activate(t, transfer, nil)
	defer worker.Close(t)

	mysql.ExecuteMySQLStatementsLineByLine(t, IncrementStatements, mysql.NewMySQLConnectionParams(t, Source.ToStorageParams()))

	srcStorage, dstStorage := mysql.NewMySQLStorageFromSource(t, Source), mysql.NewMySQLStorageFromTarget(t, Target)
	require.NoError(t, storage.WaitEqualRowsCountDifferentSchemas(t, Source.Database, Target.Database, "test", srcStorage, dstStorage, 30*time.Second))
	dumpSrc := mysql.MySQLDump(t, Source.ToStorageParams())
	dumpDst := mysql.MySQLDump(t, Target.ToStorageParams())
	canon.SaveJSON(t, map[string]interface{}{"src": dumpSrc, "dst": dumpDst})
}
