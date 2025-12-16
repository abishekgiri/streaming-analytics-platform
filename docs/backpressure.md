# Backpressure & Scaling

## Detection
We monitor backpressure via Flink Metrics:
- `isBackPressured`: If boolean true for long periods.
- `outPoolUsage`: High usage indicates downstream bottleneck.
- `checkpointAlignmentTime`: High time means barriers are moving slowly.

## Mitigation Strategies
### 1. Sink Bottleneck (Most Common)
- **ClickHouse**: Increase batch size (don't write per event!). Use `AsyncInsert` or buffer in Flink.
- **Iceberg**: Ensure commit interval isn't too frequent (e.g., 1-5 mins), not seconds.

### 2. Skew
- **Key Skew**: If partitioning by `user_id` and one user has 1000x events.
- **Fix**: Use Two-Phase aggregation (Local + Global) in Flink.

### 3. Serialization
- Use Avro specific records (code gen) instead of Generic/Reflection for speed.

## Autoscaling
- **Kafka**: Add partitions (requires job restart usually).
- **Flink**: Increase parallelism (`parallelism.default`).
