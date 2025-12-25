# Monitoring and Alerting

{{ data-transfer-name }} offers built-in monitoring tools to track pipeline health and performance:

## Key Metrics
- Delivery Lag: Measures data freshness.
- Traffic: Tracks data throughput from source to target.
- Insertion Speed: Analyzes target database performance.
- Memory Consumption: Monitors in-flight data usage.

## Alerting
Users can configure alerts based on metrics to identify and resolve issues proactively.

To provide convenient service, we measure replication system metrics. Metrics from left to right:

![alt_text](../_assets/demo_grafana_dashboard.png "image_tooltip")

* `Lag delivery ` \
  Distribution of lines on the delivery lag. The configured timestamp is taken as the starting point. If not specified - Write the Time from the source (recording time). \

* `Traffic` \
  The amount of traffic read from the log broker after decompression but before parsing. \

* `Memory consumption` \
  Shows how much data is "in flight"; it exists in the source or worker but has not yet received a response from the target sink. \

* `Parsed VS not parsed` \
  Shows the ratio of the rows (in number), how many were parsed, and how many were not parsed. Also, it tracks how many records skipped from table exclusion rules (this row goes to the `not parsed `counter). \

* `Insertion speed` \
  Distribution of rows by insertion time. It measures how long it took to insert this line into the target sink (from when parsing is completed until the target response). \

* `Maximum delivery lag` \
  A single number of seconds representing the oldest row replicated to a sink.

All these metrics describe the health of the replication process. We design an SLO that is based on values in these metrics and measure each violation of it. For example, we expect each table to be replicated in 10 seconds in the 95 percentile on a day scale.

