package case1

import (
	"testing"
	"time"

	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/tests/e2e/mongo2yt/rotator"
	"go.ytsaurus.tech/yt/go/ypath"
)

func TestCases(t *testing.T) {

	// fix time with modern but certain point
	// Note that rotator may delete tables if date is too far away, so 'now' value is strongly recommended
	ts := time.Now()

	table := abstract.TableID{Namespace: "db", Name: "test"}
	dayRotationExpectedTable := rotator.DayRotation.AnnotateWithTime("db_test", ts)

	t.Run("cleanup=drop;rotation=day;use_static_table=true;table_type=dynamic", func(t *testing.T) {
		t.Skip("TODO failing test skipped: fix with TM-5114")
		source, target := rotator.PrefilledSourceAndTarget()
		target.Cleanup = model.Drop
		target.Rotation = rotator.DayRotation
		target.UseStaticTableOnSnapshot = true
		target.Static = false
		expectedPath := ypath.Path(target.Path).Child(dayRotationExpectedTable)
		rotator.ScenarioCheckActivation(t, source, target, table, ts, expectedPath)
	})
}
