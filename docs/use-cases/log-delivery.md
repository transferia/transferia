# Log Delivery

{{ data-transfer-name }}  simplifies structured log delivery across systems. Logs are captured in Apache Kafka and transferred to various targets.

## Example: Kafka to ClickHouse
- Source: Application logs in JSON format in Kafka topics.
- Target: ClickHouse for structured storage.

### Key Features
- Horizontal scaling with independent workers per partition.
- Delivery lag as low as 0.5 seconds (95th percentile).
- User-configurable monitoring and alerting tools.
