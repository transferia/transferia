package changeitem

type Kind string

const (
	DropTableKind     = Kind("drop_table")
	TruncateTableKind = Kind("truncate")

	// The following kinds are used during the snapshot phase
	// For each table being uploaded the following ChangeItems should be emited in order
	// InitShardedTableLoad -> N * (InitTableLoad -> row events -> DoneTableLoad) -> DoneShardedTableLoad
	// where N is number of table parts if table is sharded (thus N = 1 if table is not sharded)
	// Each ChangeItem of those kinds should contain TableSchema for the corresponding table

	// InitShardedTableLoad is table header.
	// All Init/DoneTableLoad and data items for the same table should be sent strictly after this kind of item
	InitShardedTableLoad = Kind("init_sharded_table_load")
	// InitTableLoad is table part header. It should be sent for each table part before uploading any data for that part.
	// If the table is not sharded, it is considered to have a single part so InitTableLoad should be sent
	// before uploading the whole table.
	InitTableLoad = Kind("init_load_table")
	// DoneTableLoad is table part trailer. It should be sent for each table part after all data events has been sent.
	// If the table is not sharded, it is considered to have a single part so DoneTableLoad should be sent
	// after uploading the whole table.
	DoneTableLoad = Kind("done_load_table")
	// DoneShardedTableLoad is table trailer. No table control or row items should be sent for the table after this item.
	DoneShardedTableLoad = Kind("done_sharded_table_load")

	InsertKind                 = Kind("insert")
	UpdateKind                 = Kind("update")
	DeleteKind                 = Kind("delete")
	PgDDLKind                  = Kind("pg:DDL")
	DDLKind                    = Kind("DDL")
	MongoUpdateDocumentKind    = Kind("mongo:update_document")
	MongoCreateKind            = Kind("mongo:create")
	MongoDropKind              = Kind("mongo:drop")
	MongoRenameKind            = Kind("mongo:rename")
	MongoDropDatabaseKind      = Kind("mongo:dropDatabase")
	MongoNoop                  = Kind("mongo:noop")
	ChCreateTableKind          = Kind("ch:createTable")
	ClickhouseDDLBuilderKind   = Kind("ch:DDLBuilder")
	ElasticsearchDumpIndexKind = Kind("es:dumpIndex")
	SynchronizeKind            = Kind("")
)
