# Testing in Transferia

Testing is a crucial part of Transferia development. We follow a comprehensive testing strategy that ensures reliability and maintainability of our data transfer solutions.

## Testing Philosophy

Our testing approach is based on these key principles:

1. **Prefer Real Dependencies**:
   - We avoid mocks and complex test doubles whenever possible
   - If we can test with a real dependency, we prefer that approach
   - We use test-containers for dependency setup
   - If we can't use a real dependency, we prefer to skip testing rather than mock

2. **Test Pyramid**:
   - We follow the diamond test pyramid approach
   - Focus on integration tests with real dependencies
   - Minimal unit tests, only when necessary
   - Comprehensive E2E tests for critical paths

3. **Test Requirements**:
   - Zero tolerance for flaky tests in pre-commit
   - All tests must pass before merging (trunk-based development)
   - Every code change requires corresponding tests
   - Tests must run locally without external dependencies

4. **Test Infrastructure**:
   - Use plain Go code for tests when possible
   - Use test-containers for dependency setup
   - Support local development environment
   - Minimal external dependencies

## Test Types

Transferia uses several types of tests:

1. **Canonical Tests** - Tests that verify provider behavior by comparing output with expected results
2. **Integration Tests** - Tests running on local control-plane with minimal dependencies
3. **Test Container Recipes** - Integration tests using testcontainers
4. **E2E Tests** - End-to-end tests for complete data transfer scenarios

### Canonical Tests

Canonical tests verify the behavior of providers by comparing their output with expected results. These tests are located in `tests/canon/` directory.

#### Structure

```
tests/canon/
├── mongo/      # MongoDB provider tests
├── parser/     # Parser tests
├── sequences/  # Sequence handling tests
└── ...
```

#### Example Canonical Test

```go
func TestMongoSource(t *testing.T) {
    // Setup test environment
    ctx := context.Background()
    container := mongo.NewContainer()
    defer container.Terminate(ctx)

    // Create test data
    data := []ChangeItem{
        // ... test data ...
    }

    // Run test
    source, err := NewSource(container.Config())
    require.NoError(t, err)

    // Verify results
    result, err := source.Read(ctx)
    require.NoError(t, err)
    assert.Equal(t, data, result)
}
```

### Test Container Recipes

Test container recipes provide reusable test environments using testcontainers. They are located in `tests/tcrecipes/` directory.

#### Available Recipes

1. **Databases**:
   - PostgreSQL
   - ClickHouse
   - YDB
   - MySQL
   - Greenplum

2. **Message Brokers**:
   - Kafka
   - Event Hub (Azure)

3. **Storage**:
   - Object Storage
   - LocalStack (AWS services)

4. **Orchestration**:
   - K3s (lightweight Kubernetes)
   - Temporal (workflow engine)

#### Provider-Specific Helpers

Some providers have dedicated test helpers that simplify testing:

1. **MySQL Helpers** (`tests/helpers/mysql_helpers.go`):
```go
// Create MySQL source with test container
source := helpers.RecipeMysqlSource(t)

// Execute SQL statements
helpers.ExecuteMySQLStatement(t, `
    CREATE TABLE test_table (
        id INT PRIMARY KEY,
        name VARCHAR(255)
    );
`, connectionParams)

// Create storage from source
storage := helpers.NewMySQLStorageFromSource(t, source)

// Dump database state
dump := helpers.MySQLDump(t, storageParams)
```

2. **PostgreSQL Helpers**:
```go
// Create PostgreSQL container with Debezium support
container := postgres.NewContainer(ctx)
defer container.Terminate(ctx)

// Execute SQL
err := container.ExecSQL(ctx, `
    CREATE TABLE test_table (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255)
    );
`)

// Get connection parameters
config := container.Config()
```

3. **ClickHouse Helpers**:
```go
// Create ClickHouse container
container := clickhouse.NewContainer(ctx)
defer container.Terminate(ctx)

// Execute query
result, err := container.Query(ctx, `
    SELECT * FROM test_table
`)

// Get connection details
host := container.Host()
port := container.Port()
```

### End-to-End Tests

E2E tests are a key testing tool in Transferia. They allow verifying the complete data transfer cycle between different systems in conditions as close to real as possible.

#### Benefits

1. **Easy to Write**:
   - Rich infrastructure with ready-to-use containers and helpers
   - Minimal mocks and stubs
   - Direct verification of real behavior

2. **Reliability**:
   - Testing real interactions
   - Verifying complete data transfer cycle
   - Detecting integration issues

3. **Convenience**:
   - Fast local execution
   - Isolated environment
   - Automatic resource cleanup

#### Structure

E2E tests are organized by source-destination pairs in the `tests/e2e/` directory:

```
tests/e2e/
├── pg2ch/      # PostgreSQL -> ClickHouse
├── mysql2ch/   # MySQL -> ClickHouse
├── ydb2yt/     # YDB -> YT
└── ...
```

Each test typically contains:
1. `check_db_test.go` - main test for data transfer verification
2. `init_*.sql` - SQL scripts for data initialization

#### Example E2E Test

Here's an example of a simple E2E test for data transfer from PostgreSQL to ClickHouse:

```go
package pg2ch

import (
    "context"
    "testing"

    "github.com/stretchr/testify/require"
    "github.com/transferia/transferia/pkg/abstract"
    "github.com/transferia/transferia/pkg/abstract/model"
    "github.com/transferia/transferia/pkg/providers/clickhouse/model"
    chrecipe "github.com/transferia/transferia/pkg/providers/clickhouse/recipe"
    "github.com/transferia/transferia/pkg/providers/postgres"
    "github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
    "github.com/transferia/transferia/tests/helpers"
)

var (
    // Global configuration variables
    databaseName = "public"
    TransferType = abstract.TransferTypeSnapshotOnly
    
    // Source configuration (PostgreSQL)
    Source = pgrecipe.RecipeSource(
        pgrecipe.WithInitDir("dump/pg"), // Directory with SQL initialization scripts
    )
    
    // Target configuration (ClickHouse)
    Target = *chrecipe.MustTarget(
        chrecipe.WithInitDir("dump/ch"), // Directory with SQL initialization scripts
        chrecipe.WithDatabase(databaseName),
    )
)

func init() {
    // Environment initialization
    helpers.InitSrcDst(
        helpers.TransferID,
        Source,
        &Target,
        TransferType,
    )
}

func TestSnapshot(t *testing.T) {
    // Connection verification
    defer func() {
        require.NoError(t, helpers.CheckConnections(
            helpers.LabeledPort{Label: "PG source", Port: Source.Port},
            helpers.LabeledPort{Label: "CH target Native", Port: Target.NativePort},
            helpers.LabeledPort{Label: "CH target HTTP", Port: Target.HTTPPort},
        ))
    }()

    // Create transfer configuration
    transfer := helpers.MakeTransfer(
        helpers.TransferID,
        Source,
        &Target,
        TransferType,
    )

    // Get table list
    tables, err := tasks.ObtainAllSrcTables(transfer, helpers.EmptyRegistry())
    require.NoError(t, err)

    // Load data
    snapshotLoader := tasks.NewSnapshotLoader(
        client2.NewFakeClient(),
        new(model.TransferOperation),
        transfer,
        helpers.EmptyRegistry(),
    )
    err = snapshotLoader.UploadTables(
        context.Background(),
        tables.ConvertToTableDescriptions(),
        true,
    )
    require.NoError(t, err)

    // Verify results
    require.NoError(t, helpers.CompareStorages(
        t,
        Source,
        Target,
        helpers.NewCompareStorageParams().WithEqualDataTypes(PG2CHDataTypesComparator),
    ))
}
```

#### Data Initialization

SQL scripts are used to initialize test data:

```sql
-- dump/pg/init.sql
CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    value INTEGER
);

INSERT INTO test_table (name, value) VALUES
    ('test1', 1),
    ('test2', 2);
```

```sql
-- dump/ch/init.sql
CREATE TABLE test_table (
    id Int32,
    name String,
    value Int32
) ENGINE = MergeTree()
ORDER BY id;
```

## Test Helpers

Various utilities are available in `tests/helpers/`:

1. **Data Operations**:
   - `LoadTable` - load test data
   - `CompareStorages` - compare data between systems
   - `ChangeItemHelpers` - work with data changes

2. **Container Management**:
   - `mysql_helpers.go` - MySQL helpers
   - `ydb.go` - YDB helpers
   - `gp_helpers.go` - Greenplum helpers

3. **Verification**:
   - `CheckConnections` - verify connections
   - `CompareStorages` - compare storages
   - `Canonization` - data canonization

## Best Practices

1. **Test Organization**:
   - Group tests by source-destination pairs
   - Use common helpers
   - Document dependencies

2. **Data Management**:
   - Use realistic data
   - Test edge cases
   - Test different data types

3. **Resource Management**:
   - Use `defer` for cleanup
   - Verify connections
   - Clean up test data

4. **Verification**:
   - Verify data integrity
   - Test error handling
   - Validate system state

## Running Tests

1. **All E2E Tests**:
   ```bash
   go test ./tests/e2e/...
   ```

2. **Specific Test**:
   ```bash
   go test ./tests/e2e/pg2ch/...
   ```

## Next Steps

- Explore [Test Helpers](../tests/helpers)
- Check out [Examples](../../examples)
- Learn about [Architecture](./index.md) 
