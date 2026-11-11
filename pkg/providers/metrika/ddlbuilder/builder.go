package ddlbuilder

// SnapshotDDLProperty carries the source DDL on the first schema column.
// It is a string so schemas remain serializable across snapshot workers.
const SnapshotDDLProperty = "metrika_clickhouse_snapshot_ddl"

type Builder interface {
	IsMetrikaDDLBuilder()
}

type ClickHouseBuilder interface {
	Builder
	BuildClickHouseDDL(db, table, cluster string, distributed bool) (string, error)
}
