package codes

import "github.com/transferia/transferia/pkg/errors/coded"

var (

	// generic
	Dial                     = coded.Register("generic", "dial_error")
	GenericNoPKey            = coded.Register("generic", "no_primary_key")
	InvalidCredential        = coded.Register("generic", "invalid_credentials")
	MissingData              = coded.Register("generic", "missing_data")
	NetworkUnreachable       = coded.Register("generic", "network", "unreachable")
	UnknownCluster           = coded.Register("generic", "unknown_cluster")
	InvalidObjectIdentifier  = coded.Register("generic", "invalid_object_identifier")
	NoTablesFound            = coded.Register("generic", "no_tables_found")
	ShardedTransferTmpPolicy = coded.Register("generic", "sharded_transfer_tmp_policy")

	// airbyte
	AirbyteConnectionFailed = coded.Register("airbyte", "connection_failed")

	// data
	DataOutOfRange        = coded.Register("data", "out_of_range")
	UnsupportedConversion = coded.Register("data", "unsupported_type_conversion")
	DataValueError        = coded.Register("data", "value_error")

	// mysql
	MySQLDNSResolutionFailed       = coded.Register("mysql", "dns_resolution_failed")
	MySQLUnknownDatabase           = coded.Register("mysql", "unknown_database")
	MySQLDecimalNotAllowed         = coded.Register("mysql", "decimal_not_allowed")
	MySQLIncorrectSyntax           = coded.Register("mysql", "incorrect_syntax")
	MySQLBinlogFirstFileMissing    = coded.Register("mysql", "binlog_first_file_missing")
	MySQLBinlogTransactionTooLarge = coded.Register("mysql", "binlog_tx_too_large")
	MySQLSourceIsNotMaster         = coded.Register("mysql", "source_is_not_master")
	MySQLDeadlock                  = coded.Register("mysql", "deadlock")

	// opensearch
	OpenSearchInvalidDocumentKeys      = coded.Register("opensearch", "invalid_document_keys")
	OpenSearchMapperParsingException   = coded.Register("opensearch", "mapper_parsing_exception")
	OpenSearchSSLRequired              = coded.Register("opensearch", "ssl_required")
	OpenSearchTotalFieldsLimitExceeded = coded.Register("opensearch", "total_fields_limit_exceeded")

	// postgres
	PostgresAllHostsUnavailable             = coded.Register("postgres", "all_hosts_unavailable")
	PostgresDDLApplyFailed                  = coded.Register("postgres", "ddl_apply_failed")
	PostgresDropTableWithDependencies       = coded.Register("postgres", "drop_table_with_dependencies")
	PostgresDNSResolutionFailed             = coded.Register("postgres", "dns_resolution_failed")
	PostgresDuplicateKeyViolation           = coded.Register("postgres", "duplicate_key_violation")
	PostgresGeneratedColumnWriteAttempt     = coded.Register("postgres", "generated_column_write_attempt")
	PostgresInvalidSnapshot                 = coded.Register("postgres", "invalid_snapshot_identifier")
	PostgresNoPrimaryKeyCode                = coded.Register("postgres", "no_primary_key")
	PostgresObjectInUse                     = coded.Register("postgres", "object_in_use")
	PostgresReplicationConnectionNotAllowed = coded.Register("postgres", "replication_connection_not_allowed")
	PostgresReplicationSlotsInUse           = coded.Register("postgres", "replication_slots_in_use")
	PostgresSchemaDoesNotExist              = coded.Register("postgres", "schema_does_not_exist")
	PostgresSessionDurationTimeout          = coded.Register("postgres", "session_duration_timeout")
	PostgresSlotByteLagExceedsLimit         = coded.Register("postgres", "slot_byte_lag_exceeds_limit")
	PostgresSSLVerifyFailed                 = coded.Register("postgres", "ssl_verify_failed")
	PostgresTooManyConnections              = coded.Register("postgres", "too_many_connections")
	PostgresUndefinedFunction               = coded.Register("postgres", "undefined_function")
	PostgresWalSegmentRemoved               = coded.Register("postgres", "wal_segment_removed")
	PostgresPgDumpPermissionDenied          = coded.Register("postgres", "pg_dump_permission_denied")

	// runtime
	RuntimePodRestart = coded.Register("runtime", "pod_restart")

	// transformer
	FilterColumnsEmpty = coded.Register("transformer", "filter_columns_empty")

	// ycdbaas
	YcDBAASNoAliveHosts = coded.Register("ycdbaas", "no_alive_hosts")
	MDBNotFound         = coded.Register("mdb", "not_found")

	// ydb
	YDBNotFound   = coded.Register("ydb", "not_found")
	YDBOverloaded = coded.Register("ydb", "overloaded")

	// ytsaurus
	YTSaurusNotFound              = coded.Register("yt", "not_found")
	YTSaurusGenericError          = coded.Register("yt", "generic_error")
	YTSaurusOOMKilled             = coded.Register("yt", "oom_killed")
	YTSaurusProcessExitedWithCode = coded.Register("yt", "process_exited_with_code")
	YTSaurusJobsFailed            = coded.Register("yt", "jobs_failed")
	YTSaurusTooManyOperations     = coded.Register("yt", "too_many_operations")
	YTSaurusAuthorizationError    = coded.Register("yt", "authorization_error")
	// greenplum
	GreenplumExternalUrlsExceedSegments = coded.Register("greenplum", "external_urls_exceed_segments")

	// clickhouse
	ClickHouseToastUpdate         = coded.Register("ch", "update_toast_error")
	ClickHouseSSLRequired         = coded.Register("ch", "ssl_required")
	ClickHouseInvalidDatabaseName = coded.Register("ch", "invalid_database_name")

	// mongo
	MongoBSONObjectTooLarge             = coded.Register("mongo", "bson_object_too_large")
	MongoCollectionKeyTooLarge          = coded.Register("mongo", "collection_key_too_large")
	MongoServerSelectionFailed          = coded.Register("mongo", "server_selection_failed")
	MongoChangeStreamHistoryLost        = coded.Register("mongo", "change_stream_history_lost")
	MongoInvalidDeprecatedBinarySubtype = coded.Register("mongo", "invalid_deprecated_binary_subtype")
	MongoDNSResolutionFailed            = coded.Register("mongo", "dns_resolution_failed")
	MongoNonShardable                   = coded.Register("mongo", "non_shardable")

	// yc
	YCTopologyNoCommonZone                = coded.Register("yc", "topology_no_common_zone")
	YCTopologySubnetAddressSpaceCollision = coded.Register("yc", "subnet_address_space_collision")
	YCSecurityGroupsDoNotMatch            = coded.Register("yc", "subnet_security_groups_do_not_match")

	// unspecified
	Unspecified = coded.Register("unspecified")
)
