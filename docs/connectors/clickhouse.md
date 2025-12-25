---
title: "{{ CH }} connector"
description: "Configure the {{ CH }} connector to transfer data to and from {{ CH }} databases with {{ DC }} {{ data-transfer-name }}"
---

# {{ CH }} connector

You can use the {{ CH }} connector in both **source** and **target** endpoints.
In source endpoints, the connector retrieves data from {{ CH }} databases.
In target endpoints, it inserts data to {{ CH }} databases.

## Source endpoint

{% list tabs %}

* Configuration

    To configure a {{ CH }} source endpoint, provide the following settings:

    1. In **Connection type**, select the type of the {{ CH }} cluster you want to connect to.

    1. Configure the connection:

        {% cut "On-premise" %}

        Connect to an on-premise {{ CH }} installation.

        1. Under **Shards**:

            1. Click **+ Shard** and enter the shard ID.

            1. Under **Hosts**,
                click **+ Host** and enter the domain name (FQDN) or IP address of the host.

        1. In **HTTP Port**,
            enter the port for HTTP interface connections or leave the default value of `8443`.

            {% note tip %}

            * Optional fields have default values if these fields are specified.

            * [Complex types](../concepts/data-type-system.md) recording is supported (`array`, `tuple`, etc.).

            {% endnote %}

        1. In **Native port**, enter the port for 
            [clickhouse-client ![external link](../_assets/external-link.svg)](https://clickhouse.tech/docs/en/interfaces/cli/)
            connections or leave the default value of `9440`.

        1. Enable **SSL** if you need to secure your connection.

        1. To encrypt data transmission, upload a `.pem` file with certificates under **CA Certificate**.

        1. In **User** and **Password**, enter the user credentials to connect to the database.

        {% endcut %}

    1. In **Database**, enter the name of the database you want to transfer data from.

    1. Under **Table filter**, add tables you want to include or exclude:

        * **Included tables**: {{ data-transfer-name }} will transfer data only from these tables.

        * **Excluded tables**: Data from these tables won’t be transferred.

* Source data type mapping

   | **{{ CH }} type** | **{{ data-transfer-name }} type** |
   |---|---|
   | `Int64` | int64 |
   | `Int32` | int32 |
   | `Int16` | int16|
   | `Int8` | int8 |
   | `UInt64` | uint64 |
   | `UInt32` | uint32 |
   | `UInt16` | uint16|
   | `UInt8` | uint8 |
   | — | float |
   | `Float64` | double |
   |`FixedString`, `String` | string |
   | `IPv4`, `IPv6`, `Enum8`, `Enum16` | utf8 |
   | — | boolean |
   | `Date` | date |
   | `DateTime` | datetime |
   | `DateTime64` | timestamp |
   | `REST`... | any |

{% endlist %}

## Target endpoint

{% list tabs %}

* Configuration

    To configure a target {{ CH }} endpoint, provide the following settings:

    1. In **Connection type**, select the type of the {{ CH }} cluster you want to connect to.

    1. Configure the connection:

        {% cut "On-premise" %}

        Connect to an on-premise {{ CH }} installation.

        1. Under **Shards**:

            1. Click **+ Shard** and enter the shard ID.

            1. Under **Hosts**,
                click **+ Host** and enter the domain name (FQDN) or IP-address of the host.

        1. Enter **HTTP Port** for HTTP interface connections leave the default value of `8443`.

            {% note tip %}

            * Optional fields have default values if these fields are specified.

            * Complex types recording is supported (`array`, `tuple`, etc.).

            {% endnote %}

        1. In **Native port**, enter the port for
            [clickhouse-client ![external link](../_assets/external-link.svg)](https://clickhouse.tech/docs/en/interfaces/cli/)
            connections or leave the default value of `9440`.

        1. Enable **SSL** if you need to secure your connection.

        1. To encrypt data transmission, upload a `.pem` file with certificates under **CA Certificate**.

        1. In **User** and **Password**, enter the user credentials to connect to the database.

        {% endcut %}

    1. In **Database**, enter the name of the database you want to transfer data to.

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
            [TRUNCATE ![external link](../_assets/external-link.svg)](https://clickhouse.com/docs/en/sql-reference/statements/truncate/)
            command for the target table each time you run a transfer.

        {% endcut %}

    1. In **Sharding settings**, select how you want to split the data between shards.

        {% cut "Sharding by column value" %}

        1. Enter the name of a column whose values are used as the row sharding key.

        1. (Optional) If you want to map a column value to a specific shard,
            click **+ Mapping**.
            Enter the column value and the shard name.

            By default, the target endpoint uses automatic sharding.

        {% endcut %}

        {% cut "Sharding by transfer ID" %}

        1. (Optional) If you want to map a transfer ID to a specific shard,
            click **+ Mapping**.
            Enter the transfer ID and the shard name.

            By default, the target endpoint uses automatic sharding.

        {% endcut %}

        The **No sharding** and **Uniform random sharing** options have no configurable parameters.

    1. (Optional) Configure table renaming:

        1. Under **Advanced settings** → **Rename tables**, click **+ Table**.

        1. Enter the source and target table names.

            If the Target endpoint has the table with the same name, the data will be written into the existing table.

        {% note tip "Merge multiple tables with table renaming" %}

        You can merge data from several source tables into a single one at your transfer target.
        To do that, 
        create several **Rename table** entries with different **Source table name** values 
        and the same **Target table name** values.

        The data schemas in the source and target tables must be the same.

        {% endnote %}

    1. In **Flush interval**, specify a desired value or leave the default of 1 second.

* Target data type mapping

   | **{{ data-transfer-name }} type** | **{{ CH }} type** |
   |---|---|
   |int64|`Int64`|
   |int32|`Int32`|
   |int16|`Int16`|
   |int8|`Int8`|
   |uint64|`UInt64`|
   |uint32|`UInt32`|
   |uint16|`UInt16`|
   |uint8|`UInt8`|
   |float|`Float64`|
   |double|`Float64`|
   |string|`String`|
   |utf8|`String`|
   |boolean|`UInt8`|
   |date|`Date`|
   |datetime|`DateTime`|
   |timestamp|`DateTime64`|
   |any|`String`|

{% endlist %}

