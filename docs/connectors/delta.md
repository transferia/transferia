---
title: "Delta lake connector"
description: "Connector from delta-lake s3 compatible storage"
---

# Delta lake connector

## Overview

The Delta Lake Source Connector enables the ingestion of data from a Delta Lake stored on Amazon S3 or compatible object storage systems. It supports only **snapshot mode**, capturing a static view of the Delta Lake table at the time of ingestion. The connector is based on the S3 connector and provides flexibility in connecting to different S3-like storage services.

This document outlines the configuration options and behavior of the Delta Lake Source Connector, which can be controlled via JSON or YAML formats using the `DeltaSource` Go structure.

---

## Configuration

The Delta Lake Source Connector is configured using the `DeltaSource` structure. Below is a breakdown of each configuration field.

### JSON/YAML Example

```json
{
  "Bucket": "my-delta-lake-bucket",
  "AccessKey": "your-access-key",
  "SecretKey": "your-secret-key",
  "S3ForcePathStyle": true,
  "PathPrefix": "delta-lake-tables/",
  "Endpoint": "https://s3.amazonaws.com",
  "UseSSL": true,
  "VersifySSL": true,
  "Region": "us-east-1",
  "HideSystemCols": false,
  "TableName": "sales_data",
  "TableNamespace": "company_namespace"
}
```

### Fields

- **Bucket** (`string`): The S3 bucket that contains the Delta Lake table. This is the main storage location for the Delta Lake files.

- **AccessKey** (`string`): The access key for authenticating to the S3-compatible storage service.

- **SecretKey** (`server.SecretString`): The secret key for authenticating to the S3-compatible storage service.

- **S3ForcePathStyle** (`bool`): If set to `true`, forces the use of path-style access for S3. Useful when connecting to non-Amazon S3 services or local development environments like MinIO.

- **PathPrefix** (`string`): A prefix for the path where Delta Lake tables are stored within the bucket. Example: `delta-lake-tables/`.

- **Endpoint** (`string`): The endpoint URL of the S3-compatible storage service. For AWS, it’s typically `https://s3.amazonaws.com`, but can be different for other services or self-hosted environments.

- **UseSSL** (`bool`): If set to `true`, enables SSL for connections to the S3 service.

- **VersifySSL** (`bool`): Validates SSL certificates when connecting to S3.

- **Region** (`string`): The region where the S3 bucket is located, for example, `us-east-1`.

- **HideSystemCols** (`bool`): When set to `true`, hides the system columns `__delta_file_name` and `__delta_row_index` from the output schema. These columns are metadata fields added by Delta Lake, and hiding them simplifies the output structure.

- **TableName** (`string`): Defines the name of the table stored in the Delta Lake. Delta Lake always holds a single table, and this user-defined name is assigned to it.

- **TableNamespace** (`string`): A logical grouping or namespace for the table, typically representing an organizational structure.

---

## Ingestion Mode

### Snapshot Mode

The Delta Lake Source Connector supports only **snapshot mode**. This means that it captures a one-time, static view of the Delta Lake table at the time of ingestion. The snapshot contains all the records in the table up to that point.

- **Use Case**: The snapshot mode is ideal for initial data loading, data migrations, or periodic full-refresh data capture.

---

## Data Structure

In Delta Lake, the connector ingests a single table, as Delta Lake storage holds a single table per configuration. The structure of the ingested data mirrors the table's schema in the Delta Lake. By default, the system columns `__delta_file_name` and `__delta_row_index`, which contain file-level and row-level metadata, are included.

- If `HideSystemCols` is set to `true`, these system columns are hidden in the output, simplifying the data structure for downstream use cases.

---

## S3 Compatibility

This connector is based on the S3 connector and can work with any object storage system that is compatible with the S3 API. This includes:
- AWS S3
- MinIO
- Other S3-compatible services (e.g., DigitalOcean Spaces, Wasabi)

To connect to a non-Amazon S3 service, ensure that the `Endpoint` and `S3ForcePathStyle` settings are configured correctly.

---

## Security

The connector relies on `AccessKey` and `SecretKey` for authenticating to the S3-compatible storage. For secure transmission, you can enable SSL using the `UseSSL` and `VersifySSL` fields.

---

## Demo

TODO
