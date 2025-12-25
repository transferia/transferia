---
title: "{{ PG }} connector"
description: "Configure the {{ PG }} connector to transfer data to and from {{ PG }} databases with {{ DC }} {{ data-transfer-name }}"
---

# {{ PG }} connector

You can use the {{ PG }} connector in both **source** and **target** endpoints.
In source endpoints, the connector retrieves data from {{ PG }} databases.
In target endpoints, it inserts data to {{ PG }} databases.

## Source endpoint

{% list tabs %}

* Configuration

    {% note warning "Requirements for source {{ PG }} databases" %}

    1. The transferred data must be in tables, not views.

    1. Each table must have a primary key.

    {% endnote %}

    To configure a source {{ PG }} endpoint, provide the following settings:

    1. Configure the connection: 

        1. Under **Connection settings** → **Connection type**,
            expand **Hosts**.
           
        1. Click ![plus icon](../_assets/plus-sign.svg) 
            and enter the host's IP address or domain name (FQDN).
            To add several hosts, repeat this step.

        1. In **Port**, enter the port for connection, or leave the default value of `5432`.

        1. To encrypt data transmission, upload a `.pem` file with certificates under **CA Certificate**.

        1. In **Database**, enter the name of the database associated with the user and password.

        1. In **User** and **Password**, enter the user credentials to connect to the database.

            {% note warning %}

            The user must have a role with the `REPLICATION` attribute or be a superuser.
            [Learn more ![external link](../_assets/external-link.svg)](https://www.postgresql.org/docs/current/logical-replication-security.html)

            {% endnote %}

        {% cut "Standard {{ PG }} connection string reference" %}

        Typically, a {{ PG }} connection string looks as follows:
       
        ```shell
        postgres://<username>:<password>@<hostname>:<port>/<database>
        ```

        * `<username>:<password>`: (Optional) Authentication credentials.

        * `<hostname>`: IP-address or the domain name (FQDN) of the {{ MG }} server.

        * `<port>`: Port. Default is `5432`.

        * `<database>`: Name of the database to connect to.

        {% endcut %}

   1. Configure the **Table filter** if you need to transfer only specific tables. If you don't specify any of the settings below, all tables will be transferred.

      * **Included tables**

          {{ data-transfer-name }} will transfer only the data from these tables.
          Specify the table names together with the schema in the `schema_name.target_table` format.

         Enter `schema_name.*` to select all tables.

      * **Excluded tables**

         The data from the tables on this list won't be transferred. Specify the table names after the name of the schema containing these tables as follows: `schema_name.excluded_table`.

         Enter `schema_name.*` to select all tables.

   1. Configure **Schema migration**:

      {% note tip %}

      The default schema migration settings usually let you successfully perform a transfer. Change the settings of the initial and final stages of the transfer only if necessary.

      {% endnote %}

      During the transfer process, the database schema is transferred from the source to the destination. The service uses [pg_dump ![external link](../_assets/external-link.svg)](https://www.postgresql.org/docs/current/app-pgdump.html) to transfer the schema. You can set the migration rules for each object class separately. The possible options are *At begin*, *At end*, *Don't copy*.

      The transfer is performed in two stages:

      * **Activation stage**

         This stage is performed on activation of the transfer, before [snapshotting](../concepts/transfer-types.md#snapshot) or replication to create schema on the target.

         You can select parts of the schema to be transferred at this stage.

         By default, they include the following:

         * **Sequences**
         * **Owned sequences**
         * **Current values for sequences**
         * **Tables**
         * **Primary keys**
         * **Default values**
         * **Views**
         * **Functions**
         * **Types**
         * **Rules**
         * **Collations**
         * **Policies**

      * **Deactivation stage**

         This stage is performed at the end of the transfer upon its deactivation.

         If the transfer is constantly working in replication mode ([Replication](../concepts/transfer-types.md#replication) and [Snapshot and replication](../concepts/transfer-types.md#snapshot-and-replication) transfer types), then the final stage of the transfer will be performed only after the replication stops. You can choose which parts of the schema to migrate.

         At this stage, it's assumed that when the transfer is deactivated, there is no writing activity on the source. For complete reliability, the source is set to read-only mode. The database schema on the target is brought to a state where it will be consistent with the schema on the source.

         We recommend that you include resource-intensive operations, such as index transfers, in the final migration stage. Moving the indexes at the beginning of the transfer can slow it down.

         Default objects migrated at the end of a transfer are the following:

         * **Foreign keys**
         * **Constraints**
         * **Indexes**
         * **Triggers**

      {% note info %}

      When the transfer is restarted during the replication phase, the table schemas on the target are preserved. In this case, the service will transfer only the table schemas that aren't in the target at the restart.

      {% endnote %}

   1. Specify **Advanced** settings:

      * **Maximum WAL size for the replication slot**. Here you can set the maximum size of [Write-Ahead Log ![external link](../_assets/external-link.svg)](https://www.postgresql.org/docs/current/wal-intro.html) keeping on the replication slot. When this value is exceeded, replication stops and the replication slot is removed.

      * The name of the **Database schema for auxiliary tables**.

      * Check the **Merge inherited tables** box if you need to merge content of the tables.

      * Under **Parallel table copying settings**:

         * **Split threshold in bytes**

            This setting defines the minimum tables size in bytes starting from which the service will parallelize the tables. To the right from the input field, select the increment from the dropdown list - from `B` (bytes) up to `GB` (gigabytes).

         * **Maximum number of table parts**

            The maximum number of parts into which the service will split a table when using sharded loading.

   {% note tip "Partition replication for partitioned tables" %}

   If you need to replicate all the partitions of a partitioned table, do the following:

   * Add the table in question to the **Table filter** → **Included tables list**.

   * Check the **Advanced settings** → **Merge inherited tables** box.

   This will result in the source endpoint extracting the partitioned table without partitions - the extracted items' table name will be set to the name of the partitioned table, even if the item represents a row of a partition.

   {% endnote %}

* Source data type mapping

    | **{{ PG }} type** | **{{ data-transfer-name }} type** |
    |---|---|
    | `BIGINT` | int64 |
    | `INTEGER` | int32 |
    | `SMALLINT` | int16 |
    | — | int8 |
    | — | uint64 |
    | — | uint32 |
    | — | uint16 |
    | — | uint8 |
    | — | float |
    | `NUMERIC`, `REAL`, `DOUBLE PRECISION` | double |
    | `BIT (N)`, `BIT VARYING (N)`, `BYTEA`, `BIT`, `BIT VARYING` | string |
    | `CHARACTER VARYING`, `DATA`, `UUID`, `NAME`, `TEXT`, `INTERVAL`, `TIME WITH TIME ZONE`, `TIME WITHOUT TIME ZONE`, `CHAR`, `ABSTIME`, `MONEY` | utf8 |
    | `BOOLEAN` | boolean |
    | `DATE` | date |
    | — | datetime |
    | `TIMESTAMP WITHOUT TIME ZONE`, `TIMESTAMP WITH TIME ZONE` | timestamp |
    | `ARRAY`, `CHARACTER`, `CITEXT`, `HSTORE`, `JSON`, `JSONB`, `DATERANGE`, `INT4RANGE`, `INT8RANGE`, `NUMRANGE`, `POINT`, `TSRANGE`, `XML`, `INET`, `CIDR`, `MACADDR`, `OID`, `REST`... | any |

* Model

    ## Overview
    
    The Postgres Connector allows for efficient data ingestion from PostgreSQL databases using three main modes:
    
    - **Snapshot**: Fetches all data from a specified table.
    - **Snapshot with Cursor Column**: Fetches data in recurring intervals with a filter.
    - **Realtime CDC (Change Data Capture)**: Captures ongoing changes to the database in real time.
    
    This connector is controlled using JSON or YAML configuration based on the Go structure `PgSource`.
    
    ---
    
    ## Configuration
    
    The connector can be configured using the `PgSource` structure. Below is the detailed explanation of each configuration option.
    
    ### JSON/YAML Example
    
    ```json
    {
      "Hosts": ["host1", "host2"],
      "Database": "your-database",
      "User": "your-username",
      "Password": "your-password",
      "Port": 5432,
      "DBTables": ["table1", "table2"],
      "BatchSize": 1000,
      "SlotID": "replication_slot",
      "SlotByteLagLimit": 1000000,
      "TLSFile": "/path/to/tls/file",
      "KeeperSchema": "public",
      "CollapseInheritTables": true,
      "UsePolling": false,
      "ExcludedTables": ["table3"],
      "IsHomo": true,
      "NoHomo": false,
      "PreSteps": {},
      "PostSteps": {},
      "UseFakePrimaryKey": false,
      "IgnoreUserTypes": true,
      "IgnoreUnknownTables": true,
      "MaxBufferSize": 0,
      "ExcludeDescendants": false,
      "DesiredTableSize": 1000000,
      "SnapshotDegreeOfParallelism": 5,
      "EmitTimeTypes": false,
      "DBLogEnabled": false,
      "ChunkSize": 1000,
      "SnapshotSerializationFormat": "binary",
      "ShardingKeyFields": {
        "table1": ["id"],
        "table2": ["created_at"]
      },
      "PgDumpCommand": ["pg_dump", "-Fc", "-f", "dumpfile"]
    }
    ```
    
    ### Fields
    
    - **Host** (`string`, deprecated): Legacy field; prefer to use `Hosts`.
    
    - **Hosts** (`[]string`): List of PostgreSQL hosts.
    
    - **Database** (`string`): The name of the database from which data will be ingested.
    
    - **User** (`string`): The database user with privileges to read from the specified tables.
    
    - **Password** (`server.SecretString`): The password for the database user.
    
    - **Port** (`int`): The port to connect to the database. Default: `5432`.
    
    - **DBTables** (`[]string`): A list of tables to ingest from the database, support wildcard for table name, for example: `my_schema.*`, but not this: `my_schema.prefix_*`.
    
    - **BatchSize** (`uint32`): The maximum number of rows to buffer internally when performing replication (not applicable for snapshots).
    
    - **SlotID** (`string`): The replication slot to use for CDC.
    
    - **SlotByteLagLimit** (`int64`): The byte lag limit for the replication slot.
    
    - **TLSFile** (`string`): Content of TLS file for SSL connection
    
    - **KeeperSchema** (`string`): Schema used for metadata storage.
    
    - **CollapseInheritTables** (`bool`): Whether to collapse inherited tables into one logical table.
    
    - **UsePolling** (`bool`): Whether to use polling instead of replication connection (CDC).
    
    - **ExcludedTables** (`[]string`): A list of tables to exclude from ingestion.
    
    - **IsHomo** (`bool`): Whether this is a homogeneous (same schema and table structure) setup.
    
    - **NoHomo** (`bool`): Overrides `IsHomo`, forces heterogeneous relations.
    
    - **AutoActivate** (`bool`): Whether the connector should auto-activate after setup.
    
    - **PreSteps** (`PgDumpSteps`): Steps to run before the ingestion process begins, such as preparation of the environment, use for postgres to postgres migrations.
    
    - **PostSteps** (`PgDumpSteps`): Steps to run after ingestion, such as cleanup operations, use for postgres to postgres migrations.
    
    - **UseFakePrimaryKey** (`bool`): Use a fake primary key when the table does not have one.
    
    - **IgnoreUserTypes** (`bool`): Ignore user-defined types.
    
    - **IgnoreUnknownTables** (`bool`): If true, tables with unknown schemas are ignored.
    
    - **MaxBufferSize** (`server.BytesSize`, deprecated): No longer in use.
    
    - **ExcludeDescendants** (`bool`, deprecated): No longer in use; use `CollapseInheritTables` instead.
    
    - **DesiredTableSize** (`uint64`): Desired partition size for snapshot sharding.
    
    - **SnapshotDegreeOfParallelism** (`int`): Number of parallel snapshot parts.
    
    - **DBLogEnabled** (`bool`): If true, use DBLog snapshot instead of the common snapshot.
    
    - **ChunkSize** (`uint64`): Number of rows per chunk for DBLog snapshots. Automatically calculated if set to 0.
    
    - **SnapshotSerializationFormat** (`PgSerializationFormat`): Format for snapshot serialization. Defaults to "binary" for homogeneous PostgreSQL transfers, and "text" for others.
    
    - **ShardingKeyFields** (`map[string][]string`): Defines the sharding key fields for each table. Used for sharding data across multiple parts.
    
    - **PgDumpCommand** (`[]string`): Command to run `pg_dump`, which can be used for snapshot-based ingestion.
    
    ---
    
    ## Supported transfer types
    
    ### 1. Snapshot
    
    In the snapshot mode, the connector ingests all data from the specified tables in one go. The data is fetched based on the configuration fields `DBTables` and is not dependent on changes made to the database after the ingestion starts.
    
    - **Use Case**: One-time ingestion of static data.
    - **Performance Optimization**: Leverage `DesiredTableSize` and `SnapshotDegreeOfParallelism` to shard large tables across multiple processes.
    
    ### 2. Snapshot with Cursor Column
    
    In this mode, the connector ingests data from the specified tables based on a filter column (like a timestamp or auto-incrementing ID). The ingestion occurs at regular intervals, copying only the new data based on the value of the cursor column.
    
    - **Use Case**: Recurrent ingestion of new data with some form of time or ID-based filtering.
    
    ### 3. Realtime Change Data Capture (CDC)
    
    The CDC mode listens for real-time changes (insertions, updates, deletions) in the database. It uses the PostgreSQL replication protocol with a replication slot to capture and ingest these changes.
    
    - **Use Case**: Ongoing ingestion of live updates from the database.
    
    ---
    
    ## Advanced Configuration
    
    ### Sharding for Large Tables
    
    To optimize ingestion performance, the connector allows you to shard large tables by specifying the desired shard size (`DesiredTableSize`) and the degree of parallelism (`SnapshotDegreeOfParallelism`). The ingestion process can be parallelized across multiple workers.
    
    ### Security
    
    The connector supports secure connections via SSL by specifying the path to a TLS certificate (`TLSFile`) and configuring security groups (`SecurityGroupIDs`).
    
    ### Ignoring Specific Tables
    
    If there are tables that should not be ingested, you can list them under `ExcludedTables` to exclude them from the ingestion process.
    
    ---
    
    ## Error Handling
    
    1. **Unknown Table Schemas**: If a table's schema is unknown, the connector can be configured to either ignore it (`IgnoreUnknownTables`) or raise an error.
    2. **No Primary Key**: The connector can generate a fake primary key for tables without one by enabling the `UseFakePrimaryKey` option.
    
    ---
    
    ## Demo
    
    The Postgres  Connector offers flexible modes to ingest data either as a one-time snapshot, recurrent based on filters, or in real time via CDC. By configuring the various options available, the connector can be customized to meet different data ingestion requirements.
    
    ### Snapshot
    
    ![Made with VHS](https://vhs.charm.sh/vhs-3ETIytnxDtBmrgkcOX3ZBf.gif)
    
    Here’s a step-by-step guide for the Postgres to Clickhouse transfer process, based on your `tape` format for `vhs`:
    
    ---
    
    # Postgres to Clickhouse Transfer Guide
    
    This guide demonstrates how to validate, check, and activate a data transfer from Postgres to Clickhouse using `trcli` and verifies the data in Clickhouse.
    
    ---
    
    ## 1. **Prepare Transfer Configuration**
    
    Ensure that your transfer configuration file (`transfer.yaml`) is ready. This file contains all the settings for transferring data from Postgres to Clickhouse.
    
    ```yaml
    type: SNAPSHOT_ONLY
    src:
      type: pg
      params: |
        {
          "Hosts": ["localhost"],
          "User": "postgres",
          "Password": "password",
          "Database": "mydb",
          "Port": 5432
        }
    dst:
      type: ch
      params: |
        {
          "ShardsList": [{"Hosts": ["localhost"]}],
          "HTTPPort": 8123,
          "NativePort": 9000,
          "Database": "default",
          "User": "default",
          "Password": "ch_password"
        }
    
    ```
    
    ## 2. **Validate the Transfer**
    
    Use `trcli` to validate the configuration and ensure that everything is correct.
    
    ```bash
    trcli validate --transfer transfer.yaml --log-config=minimal
    ```
    
    - This will check the syntax and structure of the `transfer.yaml` file.
    
    ## 3. **Check Transfer Health**
    
    Run a health check on the transfer configuration to confirm all necessary resources are available and ready for activation.
    
    ```bash
    trcli check --transfer transfer.yaml --log-config=minimal
    ```
    
    - This will validate connectivity, schema compatibility, and other checks before the transfer starts.
    
    ## 4. **Activate the Transfer**
    
    Once the transfer configuration is validated and checked, activate the transfer to start the data migration process.
    
    ```bash
    trcli activate --transfer transfer.yaml --log-config=minimal
    ```
    
    - This command triggers the actual transfer of data from Postgres to Clickhouse.
    
    ## 5. **Verify Data in Clickhouse**
    
    After the transfer is activated, connect to your Clickhouse instance to verify that the data has been transferred correctly.
    
    ```bash
    clickhouse-client --host localhost --port 9000 --user default --password 'ch_password'
    ```
    
    - Log into your Clickhouse database.
    
    Now, run a query to check if the data is present:
    
    ```bash
    select * from personas;
    ```
    
    - This query checks the `personas` table for the transferred data.
    
    ---
    
    ### Tips:
    - **Transfer Configuration**: Make sure your `transfer.yaml` file is correctly configured for both Postgres and Clickhouse.
      - **Logs**: Adjust the `--log-config` option to view more detailed logs if necessary.
    
    ---

{% endlist %}

## Target endpoint

{% list tabs %}

* Configuration

    To configure a target {{ PG }} endpoint, provide the following settings:

    1. Configure the connection:

        1. Under **Connection settings** → **Connection type**,
            expand **Hosts**.

        1. Click ![plus icon](../_assets/plus-sign.svg) 
            and enter the host's IP address or domain name (FQDN).
            To add several hosts, repeat this step.

        1. In **Port**, enter the port for connection, or leave the default value of `5432`.

        1. To encrypt data transmission, upload a `.pem` file with certificates under **CA Certificate**.

        1. In **Database**, enter the name of the database associated with the user and password.

        1. In **User** and **Password**, enter the user credentials to connect to the database.

            {% note warning %}

            The user must have a role with the `REPLICATION` attribute or be a superuser.
            [Learn more ![external link](../_assets/external-link.svg)](https://www.postgresql.org/docs/current/logical-replication-security.html)

            {% endnote %}

        {% cut "Standard {{ PG }} connection string reference" %}

        Typically, a {{ PG }} connection string looks as follows:

        ```shell
        postgres://<username>:<password>@<hostname>:<port>/<database>
        ```

        * `<username>:<password>`: (Optional) Authentication credentials.

        * `<hostname>`: IP-address or the domain name (FQDN) of the {{ MG }} server.

        * `<port>`: Port. Default is `5432`.

        * `<database>`: Name of the database to connect to.

        {% endcut %}

    1. Select a **Cleanup policy** to specify
        how data in the target database is cleaned up when a transfer is activated, reactivated, or reloaded.

        {% cut "Cleanup policy reference" %}

        * **Disabled**:
            Don’t clean. Select this option if you only perform replication without copying data.

        * **Drop** (default):
            Fully delete the tables included in the transfer.
            Use this option
            to always transfer the latest version of the table schema to the target database from the source.

        * **Truncate**:
            Execute the
            [TRUNCATE ![external link](../_assets/external-link.svg)](https://www.postgresql.org/docs/current/sql-truncate.html)
            command for the target table each time you run a transfer.

        {% endcut %}

    1. If you want to transfer all the transactions with context in the source database to the target database,
        expand **Advanced settings** and enable **Save transaction boundaries**.

        {% note info %}

        This feature is in the **Preview** stage.

        {% endnote %}

* Target data type mapping

    | **{{ data-transfer-name }} type** | **{{ PG }} type** |
    |---|---|
    |int64|`BIGINT`|
    |int32|`INTEGER`|
    |int16|`SMALLINT`|
    |int8|`SMALLINT`|
    |uint64|`BIGINT`|
    |uint32|`INTEGER`|
    |uint16|`SMALLINT`|
    |uint8|`SMALLINT`|
    |float|`REAL`|
    |double|`DOUBLE PRECISION`|
    |string|`BYTEA`|
    |utf8|`TEXT`|
    |boolean|`BOOLEAN`|
    |date|`DATE`|
    |datetime|`TIMESTAMP WITHOUT TIME ZONE`|
    |timestamp|`TIMESTAMP WITHOUT TIME ZONE`|
    |any|`JSONB`|

{% endlist %}
