# ClickHouse exactly-once snapshot E2E

The Push snapshot scenarios set `ExactlyOnceSnapshot=true`. Every scenario runs
with an `Atomic` destination database and with a `Replicated` database. The fixture
starts a real ClickHouse server with an embedded Keeper on allocated ports.

The Push snapshot tests use source providers, the snapshot loader and the sink
factory. They check physical row counts without `FINAL`, the source keys and values, publication,
absence of temporary tables, and removal of the ClickHouse publication journal
from `TransferState`. SQL sources also include an empty table. These are source
compatibility tests with one destination shard and one replica; the snapshot
package's integration tests cover replica failures and multiple destination nodes.

| Source | Scenarios |
|---|---|
| PostgreSQL | Text/binary serialization; 1/2 workers; nullable values |
| MySQL | Consistent/non-consistent snapshots; 1/2 workers |
| MongoDB | Primary/secondary-preferred reads; 1/2 workers; canonical BSON documents |
| ClickHouse | Homogeneous HTTP / abstract2 compatibility; flag off/on; CSV/JSONCompact; 1/2 workers; MATERIALIZED/ALIAS columns |
| YDB | Sharded/unsharded source snapshots; 1/2 workers |
| YT | Static sorted tables; 1/2 workers; multiple source parts |
| Oracle | Consistent/non-consistent snapshots; 1/2 workers |
| Greenplum / Cloudberry | Consistent/non-consistent snapshots; 1/2 workers; segment connections |
| S3 | CSV, gzip CSV, JSONLine, Dynamo JSON, Line, Parquet, Nginx; 1/2 workers |
| Metrika | hits, hits_v2 and visits; asynchronous part enumeration; 1/2 workers |

ClickHouse to ClickHouse always keeps the existing homogeneous HTTP / abstract2
route, including when `ExactlyOnceSnapshot=true`. Its scenario checks the route,
data and original column definitions with both flag values. It does not test the
new exactly-once publication protocol or promise its guarantees for this route.

All rows and infrastructure are created by fixtures. Metrika uses the existing
in-memory export API fixture and actual exported protobuf objects in S3.
A skipped test is not proof of compatibility. Queue sources have
replication-only grants for ClickHouse;
Iceberg and Sample do not have a granted source-to-ClickHouse snapshot pair.

## Run

On the development server, run each non-Oracle leaf with:

```sh
ya make -DDATA_TRANSFER_DISABLE_CGO -DCGO_ENABLED=0 -ttt \
  transfer_manager/go/tests/e2e/ch_exactly_once_snapshot/postgres
```

Replace `postgres` with another source directory. Oracle requires its instant
client and CGO, so run it separately:

```sh
ya make -ttt transfer_manager/go/tests/e2e/ch_exactly_once_snapshot/oracle
```

Run the existing snapshot integration suites sequentially: they share fixed
ClickHouse/Keeper ports.

```sh
ya make -j1 -DDATA_TRANSFER_DISABLE_CGO -DCGO_ENABLED=0 -ttt \
  transfer_manager/go/pkg/providers/clickhouse/snapshot
```

The new E2E Keeper ports are allocated dynamically. Greenplum and Cloudberry
fixtures publish their coordinator/segment port ranges, so run that leaf once
at a time on a machine. Its recipe removes the containers it created.

## Source and recipe fixes

The source compatibility tests above use these fixes, which are reviewed separately:

- [Greenplum part IDs](https://a.yandex-team.ru/review/16872036)
- [Oracle container startup](https://a.yandex-team.ru/review/16872201)
- [S3 schemas and worker completion](https://a.yandex-team.ru/review/16872234)

Metrika integration remains in the ClickHouse snapshot PR. The Airbyte scenario
has been removed.

## Verification on the development server

The results below were recorded before the component split and the removal of
the Airbyte scenario. They are historical results, not a validation of the split.
The historical ClickHouse result also predates restoration of the homogeneous
route and does not validate the current compatibility scenario.

Verified on 2026-10-09 against the working tree based on
`7c81bb921da8f70c6bb6a038db1d963aa73c839d`. Counts below are terminal
source/mode/database scenarios, excluding parent subtests.

| Source | Scenarios (both databases) | Result |
|---|---:|---|
| postgres | 8 | PASS |
| mysql | 8 | PASS |
| mongo | 8 | PASS |
| clickhouse | 8 | PASS |
| ydb | 8 | PASS |
| yt | 4 | PASS |
| oracle | 8 | PASS (two consecutive runs after fixture fix) |
| greenplum | 16 | PASS |
| s3 | 28 | PASS |
| airbyte | 4 | PASS |
| metrika | 4 | PASS |

All 104 passing scenarios also check publication cleanup. The Oracle container
requires `SYS_NICE` for its VKTM startup process; after adding it, both fresh
runs passed all eight scenarios. Metrika now covers all three streams with
real S3/protobuf data, source DDL parameters and per-event multiplicity checks,
for both databases and worker counts. The export API is the in-memory fixture.
Its positive-sign history fixtures do not exercise cancellation of opposite
signs by VersionedCollapsingMergeTree; physical row-count checks remain enabled. PostgreSQL
DBLog is a separate cursor/WAL snapshot lifecycle and is not covered by the
normal text/binary loader cases. External Airbyte account-dependent connectors
are not claimed as verified.

Final non-Oracle source matrix: 36 suites and 152 tests passed, without skips.
Oracle: two consecutive fresh runs, 4 suites and 14 tests passed in each.

Regression checks: snapshot and its subpackages plus Mongo E2E: 538 passed,
53 existing environment-gated skips; snapshot loader/table-part provider:
62 passed; touched Airbyte/S3/ClickHouse-model packages: 108 passed.
Detailed run logs and `source-matrix.json` are retained on the development server
in `/tmp/ch-eo-e2e/`.

Metrika support follow-up: all four database/worker scenarios passed
(`metrika-support32.log`), with hits, hits_v2 and visits in every scenario.
Full follow-up regression: 40 suites, 569 tests passed, 54 existing
environment-gated skips; legacy Metrika canonical checks passed unchanged.
Final unit check after helper consolidation: 111 passed, 50 environment skips.
