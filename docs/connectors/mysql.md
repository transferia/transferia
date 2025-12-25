---
title: "{{ MY }} connector"
description: "Configure the {{ MY }} connector to transfer data from {{ MY }} databases with {{ DC }} {{ data-transfer-name }}"

---

# {{ MY }} connector

You can use this connector both for **source** and **target** endpoints.

## Source endpoint

{% list tabs %}

* Configuration

   1. Under **Connection settings** → **Connection type**, specify the connection properties:

      * The **Host** IP address or FQDN to connect to.

      * The **Port** for connection (`3306` by default).

      * **CA certificate**. Click **Upload file** to upload a certificate file (public key) in PEM format.

   1. Specify the database attributes:

      * **Database** name associated with the username and password below.

      * **User** for connection to {{ data-transfer-name }}.

      * **Password** for the database user.

   1. Configure the **Table filter** if you need to transfer specific tables. If you want to transfer all the available tables, skip this section.

      * **Included Tables**. 
          {{ data-transfer-name }} will transfer only the data from these tables.
          Specify the table names together with the database in the `database_name.target_table` format.

      * **Excluded tables**. The data from the tables on this list won't be transferred. Specify the table names after the name of the database containing these tables as follows: `database_name.excluded_table`.

      {% note tip %}

      To parse multiple tables in the sections above, use regular expressions as conditions to parse the tables available at the source.

      {% include notitle [regular-expressions](../_includes/transfers/regular-expressions.md) %}

      You can add fields to the above sections to function as multiple parsers or filters.

      {% endnote %}

   1. Configure the **Transfer schema**:

      This section allows you explicitly select the database schema items to migrate and set the specific transfer stage at which to migrate them.

      {% note warning %}

      In most cases, the default schema migration settings let you perform a successful transfer. Change the settings for the initial and final stages of the transfer only if necessary.

      {% endnote %}

      The transfer is performed in two stages:

      * **Activation stage**

         This stage executes on activation of the transfer, before [snapshot](../concepts/transfer-types.md#snapshot) or replication to create schema on the target.

         You can select parts of the schema to be transferred at this stage.

         By default, the **Tables** are transferred at start.

      * **Deactivation stage**

         This stage executes at the end of the transfer upon its deactivation.

         If the transfer is constantly working in replication mode ([Replication](../concepts/transfer-types.md) and [Snapshot and replication](../concepts/transfer-types.md) transfer types), the final stage of the transfer will be performed only after the replication stops. You can choose which parts of the schema to migrate.

         At this stage, it's assumed that when the transfer is deactivated, there is no writing activity on the source. For complete reliability, the source is set to read-only mode. The database schema on the target is brought to a state where it will be consistent with the schema on the source.

      {% note info %}

      When the transfer is restarted during the replication phase, the table schemas on the target are preserved. In this case, the service will transfer only the table schemas unavailable at the target after the restart.

      {% endnote %}

   1. Under **Advanced settings**:

      * **Database timezone**. Specified as [IANA Time Zone Database ![external link](../_assets/external-link.svg)](https://www.iana.org/time-zones) identifier. The default timezone is `Local`, it corresponds to the **{{ MY }}** server timezone.

* Model

    ## Overview
    
    The MySQL Source Connector allows for efficient data ingestion from MySQL databases into the target system. It supports various ingestion modes, including:
    
    - **Snapshot**: Fetches all data from a specified table.
      - **Snapshot with Cursor Column**: Fetches data in recurring intervals with a filter.
      - **Realtime CDC (Change Data Capture)**: Captures ongoing changes to the database in real time.
    
    The configuration for the MySQL Source Connector is controlled using JSON or YAML formats based on the `MysqlSource` Go structure.
    
    ---
    
    ## Configuration
    
    The MySQL Source Connector can be configured using the `MysqlSource` structure. Below is a detailed explanation of each configuration option.
    
    ### JSON/YAML Example
    
    ```json
    {
      "Host": "mysql-host",
      "User": "mysql-user",
      "Password": "mysql-password",
      "ServerID": 12345,
      "IncludeTableRegex": ["^important_table$", "^critical_data.*"],
      "ExcludeTableRegex": ["^temp_table$", "^backup_.*"],
      "Database": "your-database",
      "TLSFile": "/path/to/tls/file",
      "Port": 3306,
      "Timezone": "UTC",
      "BufferLimit": 1000,
      "UseFakePrimaryKey": false,
      "TrackerDatabase": "tracker-db",
      "ConsistentSnapshot": true,
      "SnapshotDegreeOfParallelism": 4,
      "AllowDecimalAsFloat": false,
      "RootCAFiles": ["/path/to/ca1.pem", "/path/to/ca2.pem"],
      "ReplicationFlushInterval": 5000000000
    }
    ```
    
    ### Fields
    
    - **Host** (`string`): The MySQL server hostname or IP address.
    
      - **User** (`string`): The MySQL username with privileges to access the specified tables.
    
      - **Password** (`server.SecretString`): The password for the MySQL user.
    
      - **ServerID** (`uint32`): The server ID to identify this MySQL server uniquely during replication and data capture processes.
    
      - **IncludeTableRegex** (`[]string`): Regular expressions to match table names to include in the data ingestion. Only tables matching these patterns will be ingested.
    
      - **ExcludeTableRegex** (`[]string`): Regular expressions to match table names to exclude from the data ingestion.
    
      - **Database** (`string`): The MySQL database from which data will be ingested.
    
      - **TLSFile** (`string`): Path to the TLS certificate file for secure connections to the MySQL database.
    
      - **Port** (`int`): The port for connecting to MySQL. Default is `3306`.
    
      - **Timezone** (`string`): Timezone of the MySQL server. This setting helps to ensure that datetime values are correctly interpreted during ingestion.
    
      - **BufferLimit** (`uint32`): Maximum number of rows that can be buffered during data ingestion. This is used to control memory usage during data transfer.
    
      - **UseFakePrimaryKey** (`bool`): Whether to generate a fake primary key for tables that do not have one, allowing for safe ingestion even without a primary key.
    
      - **PreSteps** (`MysqlDumpSteps`): Optional steps to execute before the ingestion process, such as preparing the database environment.
    
      - **PostSteps** (`MysqlDumpSteps`): Optional steps to execute after ingestion, such as cleanup or data validation.
    
      - **TrackerDatabase** (`string`): The name of the internal tracker database used to keep track of binlog reader progress.
    
      - **ConsistentSnapshot** (`bool`): Ensures consistent snapshots across multiple tables during the ingestion. This guarantees data consistency at the time of ingestion, especially useful in distributed environments.
    
      - **SnapshotDegreeOfParallelism** (`int`): Specifies the number of parallel parts the snapshot should be divided into for ingestion. This can significantly improve performance when dealing with large datasets.
    
      - **AllowDecimalAsFloat** (`bool`): Whether to allow decimal values to be interpreted as floating-point numbers during ingestion. This setting can affect how numerical precision is handled.
    
      - **PlzNoHomo** (`bool`): Forces disabling of homogeneous features, primarily used for testing and specific configurations.
    
      - **RootCAFiles** (`[]string`): List of paths to root CA files for validating SSL connections to the MySQL server.
      
      - **ReplicationFlushInterval** (`time.Duration`): Specifies the replication flush interval. Defined in nanoseconds.
        - Example: `5000000000` (5 seconds)
    
    ---
    
    ## Ingestion Modes
    
    ### 1. Snapshot Ingestion
    
    In snapshot mode, the connector fetches all data from the specified MySQL tables at once. This process copies the entire dataset without considering ongoing changes.
    
    - **Use Case**: Ideal for one-time data migrations or large-scale initial imports.
      - **Performance Optimization**: Use `SnapshotDegreeOfParallelism` to split large tables into smaller chunks for parallel ingestion.
    
    ### 2. Snapshot with Parallelism
    
    For large datasets, the connector can split the table into parts and ingest them in parallel. This can greatly speed up the ingestion process.
    
    - **Use Case**: Large tables that need to be ingested quickly.
      - **Configuration**: Set `SnapshotDegreeOfParallelism` to define the number of parallel workers for sharding.
    
    ### 3. Consistent Snapshot
    
    This mode ensures that the data ingested from multiple tables is consistent and reflects the state of the database at a single point in time.
    
    - **Use Case**: Data warehouses or systems that require consistency between multiple tables.
      - **Configuration**: Enable `ConsistentSnapshot` to use this mode.
    
    ---
    
    ## Advanced Configuration
    
    ### Secure Connections (TLS)
    
    To ensure secure connections to the MySQL server, provide the path to a TLS certificate using the `TLSFile` field. Additionally, you can specify root CA files for validating the SSL certificates with the `RootCAFiles` option.
    
    ```yaml
    TLSFile: "/path/to/tls/certificate.pem"
    RootCAFiles:
      - "/path/to/ca1.pem"
      - "/path/to/ca2.pem"
    ```
    
    ### Filtering Tables
    
    You can control which tables are included or excluded during ingestion by specifying regular expressions in `IncludeTableRegex` and `ExcludeTableRegex`.
    
    - **IncludeTableRegex**: Only tables that match these regular expressions will be ingested.
      - **ExcludeTableRegex**: Any tables that match these regular expressions will be excluded from ingestion.
    
    For example, to include all tables starting with `prod_` and exclude any table starting with `temp_`, you can configure:
    
    ```yaml
    IncludeTableRegex: ["^prod_.*"]
    ExcludeTableRegex: ["^temp_.*"]
    ```
    
    ### Fake Primary Key
    
    If a table does not have a primary key, ingestion might fail. To avoid this, you can enable `UseFakePrimaryKey` to generate a primary key during ingestion.
    
    ```yaml
    UseFakePrimaryKey: true
    ```
    
    ---
    
    ## Error Handling and Deprecated Features
    
    1. **Tracking Behavior**: The `NoTracking`, `YtTracking`, and `YdbTracking` fields have been deprecated and replaced by a unified tracking mechanism. Use `TrackerDatabase` to manage ingestion progress.
    
       2. **Table Compatibility**: If a table's structure is not compatible with the ingestion process, you can either modify the schema or exclude it using the `ExcludeTableRegex` field.
    
       3. **Homogeneous vs. Heterogeneous**: By default, the connector assumes that tables follow a homogeneous structure. If you need to force heterogeneous behavior (for testing or other reasons), use the `PlzNoHomo` option.
    
    ---

{% endlist %}

## Target endpoint

{% list tabs %}

* Configuration

   1. Under **Connection settings** → **Connection type**, specify the connection properties:

      * **IP or FQDN of the host** to connect to.

      * The **Port** for connection (`3306` by default).

      * **CA certificate**. Click **Choose a file** to upload a certificate file (public key) in PEM format or provide it as text.

      1. Specify the database attributes:

         * **Database** name associated with the username and password below.

         * **Username** for connecting to {{ data-transfer-name }}.

         * **Password** for the database user.

   1. Select a **Cleanup policy**. This policy allows you to select a way to clean up data in the target database when you activate, reactivate or reload the transfer:

      * `Don't cleanup`: Do not clean. Select this option if you only perform replication without copying data.

      * `Drop`: Fully delete the tables included in the transfer (default). Use this option to always transfer the latest version of the table schema to the target database from the source.

      * `Truncate`: Execute the [TRUNCATE ![external link](../_assets/external-link.svg)](https://dev.mysql.com/doc/refman/8.0/en/truncate-table.html) command for a target table each time you run a transfer.

   1. Specify **Advanced settings**:

      * **Database timezone**. Specified as [IANA Time Zone Database ![external link](../_assets/external-link.svg)](https://www.iana.org/time-zones) identifier. You can also set the special `Local` timezone as a string. This timezone corresponds to the **MySQL** server timezone. The default timezone is `Local`.

      * **SQL modes** [enabled on the target server ![external link](../_assets/external-link.svg)](https://dev.mysql.com/doc/refman/5.7/en/sql-mode.html). The delimiter for multiple modes is the comma (`,`).

         The default modes are the following:

         ```sql
         NO_AUTO_VALUE_ON_ZERO,NO_DIR_IN_CREATE,NO_ENGINE_SUBSTITUTION
         ```

      * Check **Disable constraints checks** if you don't need [these checks ![external link](../_assets/external-link.svg)](https://dev.mysql.com/doc/refman/8.0/en/create-table-check-constraints.html) and want to speed up the replication.

      * The **Database schema for service tables** specifies the database into which to place the tables with the service information.

{% endlist %}
