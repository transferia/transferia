# Data Migration

Migrating data between systems is a common challenge. {{ data-transfer-name }}  provides a reliable framework for transferring data across environments.

## Example: Service to ClickHouse
- Source: High-load on-premise ClickHouse clusters with terabytes of data.
- Target: ClickHouse Cluster.

### Approach
1. Gradual migration with parallel tasks for large datasets.
2. Sharded replication to handle high read/write loads.
3. Seamless cutover to the new environment.

{{ data-transfer-name }}  ensures minimal downtime and high data fidelity during migrations.
