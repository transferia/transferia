# Resolve issues with {{ DC }} {{ data-transfer-name }}

It may occur that, on a rare occasion, your {{ data-transfer-name }} service works not as expected. Our experience shows that you can fix a significant portion of these cases without a need to contact technical support.

This article covers the following topics:

**Common issues**:

* [Eliminate duplication in transfers](#eliminate-duplication-in-transfers)

**{{ PG }}**:

* [Fix "no key columns found" error](#fix-the-no-key-columns-found-error)
* [Can't insert a line into table - constraint error](#constraint-error)
* [Unable to apply DDL of type 'TABLE' - type does not exist (SQLSTATE 42704)](#type-does-not-exist)
* [New tables not added in "Snapshot and Replication" type transfer](#snapshot-and-replication-no-tables)
* [Slow snapshotting and replication with {{ PG }} connector](#slow-snapshotting-and-replication)
* [After editing an endpoint (added table to included tables list), data won't transfer anymore](#data-wont-transfer-after-endpoint-edit)
* [Failed to load schema using pg_dump - ERROR - permission denied](#failed-to-load=schema-permission-denied)
* [Snapshot loading failed - snapshot tasks failed - main uploader failed - errors detected on secondary workers](#snapshot-loading-failed-secondary-workers)
* [Tables with tsvector columns take too long to transfer](#tables-with-tsvector-columns-take-too-long-to-transfer)
* [Number of requested standby connections exceeds max_wal_senders](#number-of-requested-standby-connections-exceeds-max_wal_senders)

**{{ KF }}**:

* [Snapshot type transfer is stuck with Snapshotting status and keeps transferring the data](#snapshot-type-transfer-is-stuck-with-snapshotting-status-and-keeps-transferring-the-data)

Resolve the most frequent issues following the instructions below:

## Common issues

### Eliminate duplication in transfers

First, ensure the following:

* The target database contains no data, and all the residual data from previous transfers were removed (otherwise, this data will remain on the target untouched and potentially cause issues).

* The data contains a primary key for deduplication.

{{ DC }} {{ data-transfer-name }} relies on the target for deduplication of changes. Upon replication of a single line, it executes `update-where`, upon insertion - `insert-on-conflict`.

In case of network issues, the service can redo the same insertion several times. This means repeating the `insert-on-conflict` expression, which won't work if there's no primary key.

We recommend not excluding the primary key from the **List of objects to transfer**.

## {{ PG }}

### Fix the "no key columns found" error

It's a known issue that occurs on the older versions of the data plane. The transfer tries to replicate the `viewname` view but fails.

To fix the issue:

1. Stop the transfer.

1. Manually apply the **Don't copy** attribute to all the views in your connector:

   1. Open your transfer's **Source** endpoint and click **Edit**.

   1. Under **Schema migration** → **Views** select **Don't copy** from the drop-down menu:

   1. Click **Submit**.

   1. Do the same for your transfer's **Target** endpoint

1. Restart the transfer.

### Can't insert a line into table - constraint error {#constraint-error}

To fix this error:

1. Stop the transfer.

1. Open your transfer's **Source** endpoint and click **Edit**.

1. Under **Schema migration**, manually apply the *After data transfer* setting to **Constraints** and **Triggers**:

1. Click **Submit**.

1. Restart the transfer.

### Unable to apply DDL of type 'TABLE' - type does not exist (SQLSTATE 42704) {#type-does-not-exist}

The error shows in the logs as follows:

```shell
Warn(Activate): Unable to apply DDL of type 'TABLE', name '<schema_name>'.'<table_name>', error: 
ERROR: type "<table_name>.<type_name>" does not exist (SQLSTATE 42704)
```

The above behavior is an operational aspect of the [pg_dump](https://www.postgresql.org/docs/current/app-pgdump.html) utility used for schema transfer.

When specifying the schema in the **Included tables** section, {{ data-transfer-name }} will only copy the tables, not the {{ PG }} types from the schema. The exception are the types automatically set up upon creating a table at the target.

To work around this problem, manually create the necessary schema in the target database.

### New tables not added in "Snapshot and replication" type transfer {#snapshot-and-replication-no-tables}

There are multiple ways to fix this issue:

* Stop and then start the transfer. This will run the snapshotting once again and transfer the new tables. However, if the tables are sizeable, this may take some time.

* Manually create a new table in the target database. All the changes on the target will be automatically sent to this table.

* Create a separate **Snapshot** type transfer with the same source and target endpoints, and specify the absent tables in the **Objects to transfer** section. When this transfer is complete, all the missing tables will appear in the target database.

   {% note info %}

   To avoid the risk of duplicate entries appearing in the target database, stop your primary transfer before running the **Snapshot** type one.

   {% endnote %}

### Slow snapshotting and replication with {{ PG }} connector {#slow-snapshotting-and-replication}

{{ PG }} transfer speed depends on two main factors:

1. The snapshotting and replication protocol.

   **What affects the writing speed**

   Normally, {{ data-transfer-name }} writes to the target database via a fast copy protocol. If it encounters a conflict when writing a batch, it automatically switches to a much slower line-by-line `INSERT` writing. The more batches encounter a conflict, the slower the writing speed.

   **How to detect this**

   Open the **Logs** tab on your transfer page and find an entry with an `INFO` tag that reads as follows:

      ```shell
      Batch insert via PostgreSQL copy protocol failed; attempting to recover using plain INSERT
      ```

   **How to fix this**

   Ensure that the target database contains no data:

   1. Open your transfer's page and click **Edit**.

   1. Under **Cleanup policy**, select **Drop**.

   1. Check that no other active transfer is using the same target endpoint.

1. Reading concurrency of snapshot tables.

   Its primary key must be [serial](https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-SERIAL) to read the data from a single table concurrently.

   If your table's primary key fits the above requirement:

      1. Stop the transfer.

      1. Open your transfer's page and click **Edit**.

      1. Under **Transfer parameters** → **Runtime** → **Serverless runtime** → **Number of threads**, specify a number of concurrent reading jobs (more than `1`).

      1. Click **Submit**.

      1. Activate your transfer.

### After editing an endpoint (added table to included tables list), data won't transfer anymore {#data-wont-transfer-after-endpoint-edit}

To solve this, use manual target management.

1. Create a table at the target with a primary key but without a foreign key.

1. Open your transfer page and click **Edit**.

1. Under **Transfer parameters** → **List of objects to transfer**, add the new tables which you want to transfer.

1. Click **Submit**.

   This will cause your transfer to restart. After the transfer has started, the new data will transfer to the tables on the source.

1. Transfer the historical data to your target database.

   1. Lock the source table:

      ```sql
      lock <table_name> IN exclusive mode
      ```

   1. Dump the data from the source using the `pg_dump` utility:

      ```sh
      pg_dump(<source>)
      ```

   1. Transfer the data to the target:

      ```sh
      cat data.sql > psql (<target>)
      ```

   1. Kill the source table lock.

### Failed to load schema using pg_dump - ERROR - permission denied {#failed-to-load=schema-permission-denied}

This error is the result of insufficient rights on the source. To fix it:

1. Stop the transfer.

1. Connect to the database you want to transfer as a database owner and use the `GRANT` command to assign the privileges to the user you specified in the transfer settings:

   ```sql
   GRANT SELECT ON ALL TABLES IN SCHEMA <schema_name> TO <user_name>;
   GRANT USAGE ON SCHEMA <schema_name> TO <user_name>;
   ```

1. Restart the transfer.

### Failed to create a replication slot at source - All replication slots are in use (SQLSTATE 53400)

This error is caused by the insufficient number of slots in the target database.

Increase the number of [max_replication_slots](https://postgresqlco.nf/doc/en/param/max_replication_slots/).

### Snapshot loading failed - snapshot tasks failed - main uploader failed - errors detected on secondary workers {#snapshot-loading-failed-secondary-workers}

This can occur when a transfer uses sharded upload. It's executed on several workers (virtual machines). Most likely, one of them crashed due to insufficient RAM quota.

Don't hesitate to contact our [Technical support](mailto:support@double.cloud) to increase your RAM quota.

### Tables with tsvector columns take too long to transfer

As the binary format isn't yet supported on a driver level, the service has to transfer this data as `insert` commands. This method is significantly slower than the binary format.

If a table is sufficiently large, copying a snapshot through the `insert` commands may take an unacceptably long time.

Use the following workaround to speed up the transfer:

1. On a source, manually create a table, but replace the `tsvector` header with `text`.

1. In the target endpoint settings, under **Endpoint parameters** → **Cleanup policy** select **Disabled**.

1. Activate your transfer and wait until it receives the `Snapshotting` status. Give it enough time to replicate the data.

1. Perform the following query on the target database:

   ```sql
   ALTER TABLE <target_table_name>
   ALTER COLUMN v SET DATA TYPE tsvector
   USING
   to_tsvector(v);
   ```

### Number of requested standby connections exceeds max_wal_senders

This message indicates that the number of concurrent connections from standby servers or streaming backup clients exceeds the number set in the [max_wal_senders] parameter for your source {{ PG }} database.

By default, this parameter equals `10`.

To fix the issue, send the following query to your PostgreSQL database:

```sql
SET max_wal_senders=<number of your outgoing connections + 10>
```

## {{ KF }}

### Snapshot type transfer is stuck with Snapshotting status and keeps transferring the data

To stop this, restart the transfer. It will return to the desired behavior.
