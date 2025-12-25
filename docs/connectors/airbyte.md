---
title: "Airbyte connector"
description: "Compatibility layer between {{ data-transfer-name }} and Airbyte"
---

# Airbyte connector

## Overview

The Airbyte Source Connector serves as a compatibility layer between the Airbyte open-source connectors and the ingestion engine, enabling data ingestion via **snapshot**, **incremental snapshot**, and **replication** modes. This connector provides a bridge to the vast array of connectors available in the Airbyte ecosystem, allowing users to leverage Airbyte's connectors for various data sources and seamlessly integrate them into their data pipelines.

In this document, we will outline the configuration options, ingestion modes, and specific behaviors of the Airbyte Source Connector. The connector is controlled via JSON or YAML configurations based on the `AirbyteSource` Go structure.

---

## Configuration

The Airbyte Source Connector is configured using the `AirbyteSource` structure. Below is a breakdown of each configuration field.

### JSON/YAML Example

```yaml
Config: |
    {
      "access_token": "your_github_personal_access_token",
      "repository": "your_org/your_repo",
      "start_date": "2023-01-01T00:00:00Z",
      "branch": "main"
    }
BaseDir: "/var/lib/airbyte"
BatchSizeLimit: "10MB"
RecordsLimit: 5000
EndpointType: "API"
MaxRowSize: 1024
Image: "airbyte/source-github:latest"
```

### Fields

- **Config** (`string`): The primary configuration field for the Airbyte Source Connector. This must be a JSON string that defines the configuration for the underlying Airbyte connector (e.g., database connection details, API credentials). The format and contents of this JSON depend on the specific Airbyte connector defined by the `Image` field. Example configurations can be found in the Airbyte connector documentation.

- **BaseDir** (`string`): The base directory where Airbyte-related files are stored. This typically includes temporary files, logs, or other operational data needed for the Airbyte connector to function.

- **BatchSizeLimit** (`server.BytesSize`): Limits the size of each batch of data sent from the Airbyte connector to the ingestion engine. This helps control memory usage and optimize processing efficiency.

- **RecordsLimit** (`int`): Defines the maximum number of records to ingest in a single batch. This can be useful for managing the size of each data ingestion cycle.

- **MaxRowSize** (`int`): Defines the maximum size (in bytes) for a single row of data. This helps ensure that the connector can handle rows of varying sizes without exceeding memory limits.

- **Image** (`string`): Specifies the Docker image of the Airbyte connector to be used. This field determines which Airbyte connector is invoked (e.g., `airbyte/source-postgres:latest`, `airbyte/source-mysql:latest`). The image name is essential as it defines the underlying source of the data.

---

## Ingestion Modes

The Airbyte Source Connector supports multiple ingestion modes, leveraging the capabilities of both Airbyte connectors and the ingestion engine.

### Snapshot Mode

- **Snapshot Mode** performs a full extraction of all available data from the source. This mode is useful for initial data loading or full data refreshes. All records are ingested without any filters or conditions.

- **Use Case**: Full data migration, initial population of target tables.

### Incremental Snapshot Mode

- **Incremental Snapshot Mode** extracts only the records that have changed or been added since the last snapshot. This mode is more efficient for recurring data sync operations as it reduces the amount of data transferred.

- **Use Case**: Periodic updates to the target database, keeping the target synchronized with the source.

### Replication Mode (Real-time Change Data Capture)

- **Replication Mode** supports real-time data replication from the source system. Changes in the source system (inserts, updates, deletes) are captured and ingested in near real-time into the target system. This mode is ideal for keeping target systems continuously updated with minimal latency.

- **Use Case**: Real-time data synchronization, building streaming data pipelines.

---

## Integration with Airbyte Connectors

This connector integrates with the Airbyte ecosystem, enabling the use of Airbyte's wide variety of source connectors. Users specify which Airbyte connector to use via the `Image` field. The connector provides an interface for managing the data transfer between Airbyte connectors and the ingestion engine.

- **Config Field**: The `Config` field is the most critical part of the setup, as it holds the configuration JSON required by the Airbyte connector. Each Airbyte connector has its own configuration format, which typically includes connection details (e.g., host, port, credentials) for the data source. Users must consult Airbyte's documentation for the specific connector being used to properly populate this field.

- **Image Field**: The `Image` field defines which Airbyte source connector is being invoked. Airbyte provides connectors for various data sources such as databases (e.g., PostgreSQL, MySQL), APIs, and cloud storage systems. The value of the `Image` field should point to the appropriate Airbyte connector Docker image, for example:
    - PostgreSQL: `airbyte/source-postgres:latest`
    - MySQL: `airbyte/source-mysql:latest`
    - Stripe: `airbyte/source-stripe:latest`

---

## Special Considerations

### Snapshot and Incremental Sync

For **incremental snapshots** and **real-time replication**, Airbyte connectors typically handle the logic of determining changes (e.g., using primary keys or timestamps). The ingestion engine relies on the Airbyte connector's ability to track and send only changed records when operating in these modes.

---

## Demo

TODO
