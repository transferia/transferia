package schema

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/clickhouse/schema/engines"
)

func BuildDDLForHomoSink(ddl *TableDDL, distributed bool, isReplicatedDatabase bool, clusterName string, database string, altNames map[string]string) (string, error) {
	if ddl == nil {
		return "", xerrors.New("ddl is nil")
	}
	return engines.BuildDDLForHomoSink(
		ddl.SQL(),
		distributed,
		isReplicatedDatabase,
		clusterName,
		database,
		altNames,
		ddl.TableID(),
	)
}
