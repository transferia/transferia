# Data Warehousing

Operational databases are not optimized for heavy analytical workloads. {{ data-transfer-name }}  enables seamless data movement to specialized systems like data warehouses for efficient querying and reporting.

## Example: PostgreSQL to ClickHouse
- Source: PostgreSQL with 1TB of data and 15,000 transactions per second.
- Target: ClickHouse analytical database for data cubes.

### Key Benefits
- Reduced data delivery lag from days to seconds.
- Eliminated the need for secondary instances for reporting.
- Consistent and fresh data for analytics.
