## Airbyte provider

This is a bridge between native transfer and airbyte connector.
This adapter is ideal for scenarios where you need to synchronize data from an Airbyte-compatible source to a Transfer-compatible sink with minimal configuration.

We support source airbyte [connectors](https://docs.airbyte.com/category/sources)

This adapter enables integration between [Airbyte](https://docs.airbyte.com/using-airbyte/core-concepts/) and [Transfer](https://GitHub.com/transferia/transferia), facilitating the translation of Airbyte's core concepts into Transfer-compatible constructs for streamlined data movement and transformations.


This example showcase how to integrate data from [GitHub](https://airbyte.com/connectors/GitHub) to Clickhouse via Airbyte Connector.

## Overview

1. **GitHub Connector**: An [airbyte](https://docs.airbyte.com/integrations/sources/GitHub) GitHub api connector.
    - **PAM**: Personal access token to access to transfer opensource repo

3. **Transfer CLI**: A Go-based application that load API data from GitHub to Clickhouse.
    - **DinD**: Airbyte connectors are used through docker, so env must allowed by docker-in-docker, in this case - priviliged container run in docker compose

4. **Clickhouse**: An open source big data platform for distributed storage and processing.

## Getting Started

### Prerequisites

- Docker and Docker Compose installed on your machine.
- Personal access token, see [here](https://GitHub.com/settings/tokens)

### Setup Instructions

1. **Clone the Repository**:
   ```bash
   git clone https://GitHub.com/transferia/transferia
   cd transfer/examples/airbyte_adapter
   ```

2. **Build and Run the Docker Compose**:
   ```bash
   export MY_TOKEN=TOKEN_VALUE
   docker-compose up --build
   ```

3. **Access to Clickhouse**:
   Access to ClickHouse via CLI:
   ```bash
   clickhouse-client --host localhost --port 9000 --user default --password 'ch_password'
   ```

### Configuration Files

- **`transfer.yaml`**: Specifies the source (GitHub Airbyte) and destination (CH) settings inside docker-compose

### Exploring results

Once docker compose up and running your can explore results via clickhouse-cli


```sql

SELECT
   JSONExtractString(committer, 'login') AS committer_name,
   count(*)
FROM commits
WHERE NOT (JSONExtractString(committer, 'login') LIKE '%robot%')
GROUP BY committer_name
ORDER BY
   2 DESC,
   1 ASC

   Query id: ec30abc0-6bff-4946-ac36-cf89b318baf2

┌─committer_name─┬─count()─┐
│ laskoviymishka │      61 │
│ boooec         │      32 │
│ KosovGrigorii  │      21 │
│ DenisEvd       │      16 │
│ sssix6ix6ix    │      13 │
│ insomnioz      │       9 │
│ ovandriyanov   │       7 │
│ timmyb32r      │       6 │
│ asmyasnikov    │       1 │
│ hdnpth         │       1 │
│ torkve         │       1 │
│ wo1f           │       1 │
└────────────────┴─────────┘

12 rows in set. Elapsed: 0.036 sec.

SELECT
   title,
   created_at,
   closed_at,
   dateDiff('second', created_at, closed_at) AS time_to_close_seconds
FROM issues
WHERE closed_at IS NOT NULL
ORDER BY time_to_close_seconds DESC
   LIMIT 5

   Query id: b255473e-da9d-474a-8090-b9b67b85ab16

┌─title────────────────────────────────────────────┬──────────created_at─┬───────────closed_at─┬─time_to_close_seconds─┐
│ helm: add PodMonitor/ServiceMonitor section      │ 2024-10-21 11:17:48 │ 2024-11-28 17:04:02 │               3303974 │
│ NOTICKET: draft pq                               │ 2024-08-11 10:47:37 │ 2024-09-12 08:43:42 │               2757365 │
│ TRANSFER-638: Demo dumb perf boost               │ 2024-08-12 20:07:13 │ 2024-09-12 08:43:50 │               2637397 │
│ TRANSFER-783: Use generic parser for JSON-s      │ 2024-08-15 13:09:17 │ 2024-09-12 08:43:29 │               2403252 │
│ TRANSFER-786: Fill optTypes for missed col-names │ 2024-08-14 17:00:18 │ 2024-09-03 21:44:52 │               1745074 │
└──────────────────────────────────────────────────┴─────────────────────┴─────────────────────┴───────────────────────┘
```

### Stopping the Application

To stop the Docker containers, run:

```bash
docker-compose down
```

## Conclusion

This example provides a complete end-to-end Ingestion Solution using GitHub API, Clickhouse, and a Transfer application. You can use it to demonstrate how data can be replicated from Unstructured API Source to a Clickhouse data platform for real-time processing.
