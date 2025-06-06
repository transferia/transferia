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

	t.Run("cleanup=drop;rotation=none;use_static_table=true;table_type=static", func(t *testing.T) {
		source, target := rotator.PrefilledSourceAndTarget()
		target.Cleanup = model.Drop
		target.Rotation = rotator.NoneRotation
		target.UseStaticTableOnSnapshot = true
		target.Static = true
		expectedPath := ypath.Path(target.Path).Child("db_test")
		rotator.ScenarioCheckActivation(t, source, target, table, ts, expectedPath)
	})
}
