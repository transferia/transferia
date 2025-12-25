# MySQL to Clickhouse Example

This example showcase how to integrate data from Mysql to Clickhouse in 2 main modes:

1. Snapshot mode - will generate `MergeTree` table family
2. Replication (CDC) - will generate `ReplacingMergeTree` table family, wich will emulate updates / deletes via flagged insert

Also, we will run end to end docker compose sample with CDC real-time replication from MySQL to ClickHouse.

## Architecture Diagram

```mermaid
graph LR
   subgraph Source
      A[MySQL]
   end

   subgraph Load_Generation
      B[Load Generator]
   end

   subgraph TRCLI
      C[Replication from MySQL]
   end

   subgraph Destination
      D[Clickhouse]
   end

   B -- Generate random CRUD load --> A
   A -- CRUD Operations --> C
   C -- Replicates Data --> D

   classDef source fill:#dff,stroke:#000,stroke-width:2px,rx:5px,ry:5px;
   classDef load fill:#ffefaa,stroke:#000,stroke-width:2px,rx:5px,ry:5px;
   classDef replication fill:#aaf,stroke:#000,stroke-width:2px,rx:5px,ry:5px;
   classDef destination fill:#afa,stroke:#000,stroke-width:2px,rx:5px,ry:5px;

   class A source
   class B load
   class C replication
   class D destination

```

## Overview

1. **Mysql**: A Mysql instance is used as the source of data changes.
    - **Database**: `testdb`
    - **User**: `testuser`
    - **Password**: `testpassword`
    - **Initialization**: Data is seeded using `init.sql`.

3. **Transfer CLI**: A Go-based application that replicates changes from Mysql to YT.
    - **Configuration**: Reads changes from Mysql and sends them to Clickhouse tables.

4. **Clickhouse**: An open source big data platform for distributed storage and processing.

5. **Load Generator**: A CRUD load generator that performs operations on the Mysql database, which triggers CDC.

## Getting Started

### Prerequisites

- Docker and Docker Compose installed on your machine.

### Setup Instructions

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/transferia/transferia
   cd transfer/examples/mysql2ch
   ```

2. **Build and Run the Docker Compose**:
   ```bash
   docker-compose up --build
   ```

3. **Access to Clickhouse**:
   Access to ClickHouse via CLI:
   ```bash
   clickhouse-client --host localhost --port 9000 --user default --password 'ch_password'
   ```

### Using the Application

- Once the Docker containers are running, you can start performing CRUD operations on the Mysql database. The `load_gen` service will simulate these operations.
- The `transfer` CLI will listen for changes in the Mysql database and replicate them to YT.
- You can monitor the changes in YT using the YT UI.

### Configuration Files

- **`transfer.yaml`**: Specifies the source (Mysql) and destination (CH) settings inside docker-compose

### Exploring results

Once docker compose up and running your can explore results via clickhouse-cli


```sql

SELECT *
FROM users
WHERE __data_transfer_delete_time = 0
   LIMIT 10


┌───id─┬─email──────────────────┬─name────┬─__data_transfer_commit_time─┬─__data_transfer_delete_time─┐
│ 3269 │ updated760@example.com │ User451 │         1732118484000000000 │                           0 │
│ 3281 │ updated646@example.com │ User91  │         1732118486000000000 │                           0 │
│ 3303 │ updated89@example.com  │ User107 │         1732118485000000000 │                           0 │
│ 3332 │ updated907@example.com │ User7   │         1732118485000000000 │                           0 │
│ 3336 │ updated712@example.com │ User473 │         1732118485000000000 │                           0 │
│ 3338 │ updated993@example.com │ User894 │         1732118485000000000 │                           0 │
│ 3340 │ updated373@example.com │ User313 │         1732118484000000000 │                           0 │
│ 3347 │ updated994@example.com │ User589 │         1732118484000000000 │                           0 │
│ 3348 │ updated515@example.com │ User96  │         1732118484000000000 │                           0 │
│ 3354 │ updated35@example.com  │ User267 │         1732118485000000000 │                           0 │
└──────┴────────────────────────┴─────────┴─────────────────────────────┴─────────────────────────────┘
```

### Stopping the Application

To stop the Docker containers, run:

```bash
docker-compose down
```

## Conclusion

This example provides a complete end-to-end CDC solution using Mysql, Clickhouse, and a Transfer application. You can use it to demonstrate how data can be replicated from a relational database to a Clickhouse data platform for real-time processing.
