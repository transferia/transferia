package schema

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/clickhouse/schema/engines"
	"go.ytsaurus.tech/library/go/core/log"
)

func BuildDDLForSink(lgr log.Logger, ddl *TableDDL, distributed bool, clusterName string, database string, altNames map[string]string) (string, error) {
	if ddl == nil {
		return "", xerrors.New("ddl is nil")
	}
	ddlChangeItem := ddl.ToChangeItem()
	return engines.BuildDDLForSink(
		lgr,
		distributed,
		clusterName,
		database,
		altNames,
		ddl.SQL(),
		ddlChangeItem.Kind == abstract.ChCreateTableDistributedKind,
		ddl.TableID(),
		ddl.Engine(),
	)
}
