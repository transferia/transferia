# Event-Driven Updates

Microservices architectures often rely on materialized views for efficient queries. {{ data-transfer-name }}  enables event-driven updates for real-time synchronization.

## Example: Materialized Views for Microservices
- Source: Domain events published to Kafka.
- Target: Microservices' local databases for querying.

### Advantages
- Improved autonomy and availability for services.
- Eliminated cascading API calls with local materialized views.
- Enhanced read performance and scalability.

{{ data-transfer-name }}  integrates seamlessly with the CQRS pattern, promoting decoupled state management.
