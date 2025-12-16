# Exactly-Once Semantics

## Flink (State Consistency)
Flink uses checkpointing (Chandy-Lamport algorithm) to ensure state within the application is exactly-once.
- **Checkpoint Interval**: 10 seconds (tunable).
- **State Backend**: RocksDB (for large state) or Hash (for low latency).

## End-to-End Guarantees

### Sink: ClickHouse (Hot Path)
ClickHouse does **effectively-once** via idempotent writes.
- We use `ReplacingMergeTree` engine.
- **Dedupe Key**: `event_id` (or `(minute, campaign_id, ...)` for agg tables).
- We write aggregated data to minimize volume, using versioned upserts if needed.

### Sink: Iceberg (Cold Path)
Iceberg supports **transactional commits**.
- Flink `IcebergSink` ensures files are committed atomically.
- This provides true end-to-end exactly-once for the cold path.
