package codes

import "github.com/transferia/transferia/pkg/errors/coded"

var (
	// generic
	NetworkUnreachable = coded.Register("generic", "network", "unreachable")
	UnknownCluster     = coded.Register("generic", "unknown_cluster")
	InvalidCredential  = coded.Register("generic", "invalid_credentials")
	Dial               = coded.Register("generic", "dial_error")
	MissingData        = coded.Register("generic", "missing_data")

	// data
	DataOutOfRange        = coded.Register("data", "out_of_range")
	UnsupportedConversion = coded.Register("data", "unsupported_type_conversion")
	DataValueError        = coded.Register("data", "value_error")

	// postgres
	PostgresNoPrimaryKeyCode          = coded.Register("postgres", "no_primary_key")
	PostgresDropTableWithDependencies = coded.Register("postgres", "drop_table_with_dependencies")
	PostgresDNSResolutionFailed       = coded.Register("postgres", "dns_resolution_failed")
	PostgresSlotByteLagExceedsLimit   = coded.Register("postgres", "slot_byte_lag_exceeds_limit")
	PostgresDDLApplyFailed            = coded.Register("postgres", "ddl_apply_failed")

	// transformer
	FilterColumnsEmpty = coded.Register("transformer", "filter_columns_empty")

	// mysql
	MySQLIncorrectSyntax   = coded.Register("mysql", "incorrect_syntax")
	MySQLDeadlock          = coded.Register("mysql", "deadlock")
	MySQLDecimalNotAllowed = coded.Register("mysql", "decimal_not_allowed")
	MySQLUnknownDatabase   = coded.Register("mysql", "unknown_database")

	// airbyte
	AirbyteConnectionFailed = coded.Register("airbyte", "connection_failed")

	// ycdbaas
	YcDBAASNoAliveHosts = coded.Register("ycdbaas", "no_alive_hosts")

	// mdb
	MDBNotFound = coded.Register("mdb", "not_found")

	// ydb
	YDBNotFound = coded.Register("ydb", "not_found")

	// other
	Unspecified           = coded.Register("unspecified")
	ClickHouseToastUpdate = coded.Register("ch", "update_toast_error")
	RuntimePodRestart     = coded.Register("runtime", "pod_restart")
	GenericNoPKey         = coded.Register("generic", "no_primary_key")
)
