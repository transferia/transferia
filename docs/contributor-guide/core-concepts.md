# Core Concepts

This document explains the core concepts of Transferia.

## Transfer Types

### Snapshot Transfer

A one-time transfer of data from source to destination:

```mermaid
graph LR
    A[Source] -->|Snapshot| B[Destination]
```

Key characteristics:
- One-time operation
- Full data copy
- No change tracking
- Uses `MergeTree` tables

### Replication Transfer

Continuous transfer of changes from source to destination:

```mermaid
graph LR
    A[Source] -->|CDC| B[Destination]
    B -->|Replication| C[Replica]
```

Key characteristics:
- Continuous operation
- Change tracking
- Real-time updates
- Uses `ReplacingMergeTree` tables

### Combined Transfer

Combination of snapshot and replication:

```mermaid
graph LR
    A[Source] -->|Snapshot| B[Destination]
    B -->|Replication| C[Replica]
```

Key characteristics:
- Initial snapshot
- Followed by replication
- Consistent state
- Efficient updates

## Data Processing

### Change Items

The basic unit of data transfer:

```go
type ChangeItem struct {
    Operation OperationType
    Data      map[string]interface{}
    Schema    Schema
    Timestamp time.Time
}
```

Key features:
- Operation type (insert/update/delete)
- Data payload
- Schema information
- Timestamp

### Data Buffering

```mermaid
graph LR
    A[Source] -->|Chunks| B[Buffer]
    B -->|Processed| C[Sink]
    B -->|Backpressure| A
```

Key features:
- Memory management
- Backpressure handling
- Batch processing
- Error recovery

### State Management

```mermaid
stateDiagram-v2
    [*] --> Created
    Created --> Validating
    Validating --> Valid
    Validating --> Invalid
    Invalid --> [*]
    Valid --> Initializing
    Initializing --> Running
    Running --> Paused
    Paused --> Running
    Running --> Stopping
    Stopping --> [*]
    Running --> Error
    Error --> Retrying
    Retrying --> Running
    Retrying --> [*]
```

Key features:
- State transitions
- Error handling
- Recovery mechanisms
- Progress tracking

## Schema Management

### Schema Inference

```mermaid
graph TD
    A[Source Schema] --> B[Schema Analyzer]
    B --> C[Schema Mapper]
    C --> D[Destination Schema]
    D --> E[Schema Validator]
    E -->|Valid| F[Apply Schema]
    E -->|Invalid| G[Schema Fixer]
    G --> D
```

Key features:
- Automatic schema detection
- Schema mapping
- Schema validation
- Schema fixes

### Schema Evolution

```mermaid
graph LR
    A[Old Schema] -->|Migration| B[New Schema]
    B -->|Validation| C[Data Check]
    C -->|Success| D[Apply Changes]
    C -->|Failure| E[Rollback]
```

Key features:
- Schema versioning
- Migration support
- Data validation
- Rollback capability

## Error Handling

### Error Types

1. **Transient Errors**:
   - Network issues
   - Temporary unavailability
   - Rate limiting

2. **Permanent Errors**:
   - Schema mismatches
   - Data corruption
   - Configuration errors

### Recovery Mechanisms

```mermaid
graph TD
    A[Error] --> B{Error Type}
    B -->|Not Fatal| C[Retry]
    B -->|Fatal| D[Fail]
    C --> E{Retry Count}
    E -->|Max| D
    E -->|Available| F[Backoff]
    F --> G[Retry]
```

Key features:
- Error classification
- Retry strategies
- Backoff mechanisms
- Failure handling

## Next Steps

- Follow the [Development Workflow](./development.md)
- Explore [Advanced Topics](./advanced.md)
- Check out [Examples](../examples) 
