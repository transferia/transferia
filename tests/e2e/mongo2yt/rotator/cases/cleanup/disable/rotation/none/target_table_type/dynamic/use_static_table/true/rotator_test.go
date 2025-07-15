package case1

import (
	"os"
	"testing"
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	ytcommon "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/tests/e2e/mongo2yt/rotator"
	"go.ytsaurus.tech/yt/go/ypath"
)

func TestMain(m *testing.M) {
	ytcommon.InitExe()
	os.Exit(m.Run())
}

func TestCases(t *testing.T) {

	// fix time with modern but certain point
	// Note that rotator may delete tables if date is too far away, so 'now' value is strongly recommended
	ts := time.Now()

	table := abstract.TableID{Namespace: "db", Name: "test"}

	t.Run("cleanup=disabled;rotation=none;use_static_table=true;table_type=dynamic", func(t *testing.T) {
		source, target := rotator.PrefilledSourceAndTarget()
		target.Cleanup = model.DisabledCleanup
		target.Rotation = rotator.NoneRotation
		target.UseStaticTableOnSnapshot = true
		target.Static = false
		expectedPath := ypath.Path(target.Path).Child("db_test")
		rotator.ScenarioCheckActivation(t, source, target, table, ts, expectedPath)
	})
}
