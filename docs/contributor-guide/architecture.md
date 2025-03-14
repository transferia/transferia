# Architecture Overview

This document provides a comprehensive overview of the Transferia architecture.

## System Components

```mermaid
graph TB
    subgraph "Transferia Core"
        A[Source Plugin] --> B[Transformer]
        B --> C[Sink Plugin]
        D[Coordinator] --> A
        D --> B
        D --> C
    end

    subgraph "Data Sources"
        E[MySQL]
        F[PostgreSQL]
        G[Kafka]
        H[S3]
    end

    subgraph "Data Destinations"
        I[ClickHouse]
        J[YTSaurus]
        K[Kafka]
        L[Other DBs]
    end

    E --> A
    F --> A
    G --> A
    H --> A

    C --> I
    C --> J
    C --> K
    C --> L

    subgraph "Monitoring & Control"
        M[Metrics]
        N[Logging]
        O[Health Checks]
    end

    D --> M
    D --> N
    D --> O
```

## Data Flow

```mermaid
sequenceDiagram
    participant Source as Source Plugin
    participant Buffer as Data Buffer
    participant Transformer as Transformer
    participant Sink as Sink Plugin
    participant Coordinator as Coordinator

    Source->>Coordinator: Register operation
    Coordinator->>Source: Assign work
    loop Data Processing
        Source->>Buffer: Push data chunks
        Buffer->>Transformer: Process data
        Transformer->>Sink: Send transformed data
        Sink->>Coordinator: Report progress
    end
    Coordinator->>Source: Check completion
```

## Core Components

### Source Plugin

The Source Plugin is responsible for reading data from various data sources. It implements the following interface:

```go
type Source interface {
    Run(sink AsyncSink) error
    Stop()
}
```

Key features:
- Asynchronous data reading
- Support for different data sources
- Error handling and recovery
- Progress tracking

### Sink Plugin

The Sink Plugin writes data to various destinations:

```go
type AsyncSink interface {
    AsyncPush(items []ChangeItem) chan error
    Close() error
}
```

Key features:
- Asynchronous data writing
- Batch processing
- Error handling
- Transaction support

### Transformer

Transformers modify data during the transfer process:

```go
type Transformer interface {
    Transform(items []ChangeItem) []ChangeItem
}
```

Key features:
- Data transformation
- Schema modification
- Data validation
- Filtering

### Coordinator

The Coordinator manages the overall transfer process:

```go
type Coordinator interface {
    CreateOperationWorkers(operationID string, workersCount int) error
    GetOperationProgress(operationID string) (*model.AggregatedProgress, error)
    AssignOperationTablePart(operationID string, workerIndex int) (*model.OperationTablePart, error)
}
```

Key features:
- Worker management
- Progress tracking
- Error handling
- State management

## Plugin System

```mermaid
classDiagram
    class Source {
        <<interface>>
        +Run(sink AsyncSink) error
        +Stop()
    }

    class AsyncSink {
        <<interface>>
        +AsyncPush(items []ChangeItem) chan error
        +Close() error
    }

    class Transformer {
        <<interface>>
        +Transform(items []ChangeItem) []ChangeItem
    }

    class Coordinator {
        <<interface>>
        +CreateOperationWorkers()
        +GetOperationProgress()
        +AssignOperationTablePart()
    }

    Source --> AsyncSink
    Transformer --> AsyncSink
    Coordinator --> Source
    Coordinator --> AsyncSink
```

## Data Processing Pipeline

```mermaid
graph LR
    subgraph "Input Data"
        A[Raw Data]
        B[Schema]
    end

    subgraph "Transformation Pipeline"
        C[Parser]
        D[Transformer 1]
        E[Transformer 2]
        F[Transformer N]
    end

    subgraph "Output Data"
        G[Transformed Data]
        H[New Schema]
    end

    A --> C
    B --> C
    C --> D
    D --> E
    E --> F
    F --> G
    F --> H
```

## Error Handling

```mermaid
graph TD
    A[Error Occurs] --> B{Error Type}
    B -->|Transient| C[Retry Logic]
    B -->|Permanent| D[Error Handler]
    C --> E{Retry Count}
    E -->|Max Retries| D
    E -->|Retry Available| F[Backoff]
    F --> G[Retry Operation]
    D --> H[Log Error]
    D --> I[Notify Coordinator]
    I --> J[Update State]
```

## Next Steps

- Learn about [Core Concepts](./core-concepts.md)
- Follow the [Development Workflow](./development.md)
- Explore [Advanced Topics](./advanced.md) 