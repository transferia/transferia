package external_readers

import "context"

// ExternalReader reads an Iceberg table using a system other than transfer-manager.
// Implementations can use DuckDB, Spark, Trino, or any other Iceberg-compatible engine.
type ExternalReader interface {
	Name() string
	ReadTable(ctx context.Context, table TableReference) (*ReadResult, error)
}

type TableReference struct {
	Namespace []string
	Name      string
}

// ReadResult contains values in the textual representation returned by the reader.
// This keeps the contract independent of an engine-specific type system or driver.
type ReadResult struct {
	Columns []string
	Rows    [][]string
}

type ExternalReaderConfig struct {
	RESTCatalogURI string
	Warehouse      string

	S3Endpoint    string
	S3Region      string
	S3AccessKeyID string
	S3SecretKey   string
	S3PathStyle   bool

	// ContainerExtraHosts makes service names from the Iceberg catalog configuration
	// resolvable inside a reader container.
	ContainerExtraHosts []string
}
