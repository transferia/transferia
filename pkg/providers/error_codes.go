package providers

import "github.com/transferia/transferia/pkg/errors/coded"

var (
	NetworkUnreachable = coded.Register("generic", "network", "unreachable")
	UnknownCluster     = coded.Register("generic", "unknown_cluster")
	InvalidCredential  = coded.Register("generic", "invalid_credentials")
	Dial               = coded.Register("generic", "dial_error")

	// MissingData means that user asked for a table / topic / object wich is not exists on a source side
	MissingData = coded.Register("generic", "missing_data")

	// DataOutOfRange means data type is correct but the value is outside the supported range
	DataOutOfRange = coded.Register("data", "out_of_range")
	// UnsupportedConversion means the source data type cannot be converted into the target data type
	UnsupportedConversion = coded.Register("data", "unsupported_type_conversion")
	// DataValueError means there is something wrong with the value itself (i.e value []byte("foo") cannot be put into Decimal field)
	DataValueError = coded.Register("data", "value_error")
)
