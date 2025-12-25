---
title: "ElasticSearch connector"
description: "Configure the ElasticSearch connector to transfer data to and from ElasticSearch with {{ DC }} {{ data-transfer-name }}"
---

# ElasticSearch connector

You can use this connector for **source** and **target** endpoints.

## Source endpoint

{% list tabs %}

* Configuration

   1. Under **Connection** → **Connection type** → **Data nodes**, click **+ Nodes**.

      For each node on the source cluster, specify **Host** and **Port**.

   1. Check the **SSL** box if you want to encrypt your connection.

   1. Add the **CA Certificate**. Click **Upload file** to provide an ElasticSearch certificate file.

      For more information on how to create such certificate, see the [official ElasticSearch documentation ![external link](../_assets/external-link.svg)](https://www.elastic.co/guide/en/elasticsearch/reference/current/security-basic-setup.html#generate-certificates).

   1. Specify your **User** name.

   1. Provide the **Password** associated with the above user.

* Source data type mapping

   | **ElasticSearch type** | **{{ data-transfer-name }} type** |
   |---|---|
   | `long` | int64 |
   | `integer` | int32 |
   | `short` | int16 |
   | `byte` | int8 |
   | `unsigned_long` | uint64 |
   | — | uint32 |
   | — | uint16 |
   | — | uint8 |
   | `float`, `half_float` | float |
   | `double`, `scaled_float`, `rank_feature` | double |
   | `text`, `ip`, `constant_keyword`, `match_only_text`, `search_as_you_type` | string |
   | `IPv4` | utf8 |
   | `boolean` | boolean |
   | — | date |
   | — | datetime |
   | `date` | timestamp |
   | `REST`... | any |

{% endlist %}

## Target endpoint

{% list tabs %}

* Configuration

   1. Under **Connection** → **Connection type** → **Data nodes**, click **+ Nodes**.

      For each node on the target cluster, specify **Host** and **Port**.

   1. Check the **SSL** box if you want to encrypt your connection.

   1. Add the **CA Certificate**. Click **Upload file** to provide an ElasticSearch certificate file.

      For more information on how to create such certificate, see the [official ElasticSearch documentation ![external link](../_assets/external-link.svg)](https://www.elastic.co/guide/en/elasticsearch/reference/current/security-basic-setup.html#generate-certificates).

   1. Specify your **User** name.

   1. Provide the **Password** associated with the above user.

   1. Select the **Cleanup policy**. This policy allows you to select a way to clean up data in the target database when you activate, reactivate or reload the transfer:

      * `Don't cleanup`: Select this option if you only perform replication without copying data.

      * `Drop`: Fully delete the collections included in the transfer (default). Use this option to always transfer the latest version of the schema to the target database from the source.

      * `Truncate`: Execute the [remove() ![external link](../_assets/external-link.svg)](https://www.mongodb.com/docs/manual/reference/method/db.collection.remove/) command for a target collection each time you run a transfer.

   1. Check the **Sanitize the documents keys** box. It cleans the JSON keys in the indexed documents by removing invalid characters, leading/trailing whitespaces, and leading/trailing dots.

* Target data type mapping

   | **{{ data-transfer-name }} type** | **ElasticSearch type** |
   |---|---|
   |int64|`long`|
   |int32|`integer`|
   |int16|`short`|
   |int8|`byte`|
   |uint64|`unsigned_long`|
   |uint32|`unsigned_long`|
   |uint16|`Uunsigned_long`|
   |uint8|`unsigned_long`|
   |float|`float`|
   |double|`double`|
   |string|`text`|
   |utf8|`text`|
   |boolean|`boolean`|
   |date|`date`|
   |datetime|`date`|
   |timestamp|`date`|
   |any|`json`|

{% endlist %}
