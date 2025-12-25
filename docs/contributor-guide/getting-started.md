# Getting Started

This guide will help you set up your development environment and make your first contribution to Transferia.

## Project Setup

### Prerequisites

- Go 1.22 or later
- Docker (optional, for running examples)
- Make
- Git

### Development Environment

1. Clone the repository:
   ```bash
   git clone https://github.com/transferia/transferia.git
   cd transferia
   ```

2. Install dependencies:
   ```bash
   go mod download
   ```

3. Build the project:
   ```bash
   make build
   ```

4. Run tests:
   ```bash
   make test
   ```

## Project Structure

```
transferia/
├── cmd/           # Command-line interface implementations
├── pkg/           # Core package implementations
│   ├── abstract/  # Abstract interfaces and types
│   ├── source/    # Source plugins
│   ├── sink/      # Sink plugins
│   ├── storage/   # Storage implementations
│   └── transformer/ # Data transformers
├── internal/      # Internal implementation details
├── examples/      # Example configurations
├── docs/          # Documentation
└── tests/         # Test files
```

## First Steps

### 1. Run an Example

Let's start with a simple example of transferring data from MySQL to ClickHouse:

1. Start the example environment:
   ```bash
   cd examples/mysql2ch
   docker-compose up -d
   ```

2. Check the logs:
   ```bash
   docker-compose logs -f transfer
   ```

### 2. Explore the Code

1. Start with the main interfaces:
   - [Source](./architecture.md#source)
   - [Sink](./architecture.md#sink)
   - [Transformer](./architecture.md#transformer)

2. Look at example implementations:
   - MySQL source: `pkg/source/mysql/`
   - ClickHouse sink: `pkg/sink/clickhouse/`

### 3. Make Your First Change

1. Create a new branch:
   ```bash
   git checkout -b feature/your-feature
   ```

2. Make your changes

3. Run tests:
   ```bash
   make test
   ```

4. Submit a pull request

## Next Steps

- Read the [Architecture Overview](./architecture.md)
- Learn about [Core Concepts](./core-concepts.md)
- Follow the [Development Workflow](./development.md) 
