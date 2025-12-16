# Observability

## Key Metrics to Watch

### Kafka
- `kafka_server_BrokerTopicMetrics_MessagesInPerSec_Count`: Ingest rate.
- `kafka_consumergroup_lag`: Critical. Should be near zero.

### Flink
- `numRecordsInPerSecond`: Throughput.
- `lastCheckpointDuration`: Should be < 10s usually.
- `fullRestarts`: Should be 0.
- `outPoolUsage`: 1.0 = 100% backpressure.

### ClickHouse
- `InsertQuery`: Rate of inserts.
- `QueryDuration`: p95/p99 latency.

### Iceberg
- `committed-data-files-count`: Growth rate.

## Dashboards
- **Overview**: High-level health (Lag, Throughput, Error Rate).
- **Producers**: Per-client stats.
- **Processing**: Detailed Flink job steps.
- **Storage**: ClickHouse & MinIO disk usage.
