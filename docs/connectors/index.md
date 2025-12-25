---
title: "Transfer connectors"
description: "Explore the list of {{ data-transfer-name }} connectors in {{ DC }} and see their usage in different transfer types."
---

# Transfer connectors

{% note info %}

{{ DC }} {{ data-transfer-name }} uses custom-built connectors for the following sources:
[{{ CH }}](clickhouse.md),
[{{ PG }}](postgresql.md),
[{{ MY }}](mysql.md),
[{{ MG }}](mongodb.md),
and [{{ S3 }}](object-storage.md).

Other connectors are based on [Airbyte](https://docs.airbyte.com/integrations/).

{% endnote %}

## Available Transfer connectors

| **Name**                  | **Type**                                      |
|:--------------------------|:----------------------------------------------|
| [{#T}](airbyte.md)        | Snapshot                                      |
| [{#T}](postgresql.md)     | CDC / Snapshot / target                       |
| [{#T}](mongodb.md)        | CDC / Snapshot / target                       |
| [{#T}](mysql.md)          | CDC / Snapshot / target                       |
| [{#T}](kafka.md)          | streaming / target                            |
| [{#T}](object-storage.md) | Snapshot / target / replication / append-only |
| [{#T}](clickhouse.md)     | Snapshot / incremental / target / sharding    |
| [{#T}](ytsaurus.md)       | Snapshot / incremental / target / sharding    |
| [{#T}](kinesis.md)        | streaming                                     |
| [{#T}](elasticsearch.md)  | Snapshot / target                             |
| [{#T}](opensearch.md)     | Snapshot / target                             |
| [{#T}](delta.md)          | Snapshot                                      |
