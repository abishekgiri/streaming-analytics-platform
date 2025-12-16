# operational Runbook

## Routine Maintenance
- **Schema Updates**: Register new schemas in Registry before deploying producers.
- **Topic Cleanup**: Check `events.dlq.v1` weekly. Replay valid events if issue fixed.

## Incident Management

### Flink Job Failed
1.  Check Flink Dashboard / Logs.
2.  **Restart Strategy**: Jobs are configured to restart with fixed delay.
3.  **Manual Restart**:
    ```bash
    ./bin/flink run -s savepoint-path ...
    ```

### High Consumer Lag
1.  Check if Flink is backpressuring (see Backpressure doc).
2.  No backpressure? Flink is just slow or underprovisioned.
    - Scale up parallelism.
    - Check CPU/Memory on TaskManagers.

### ClickHouse Slow
1.  Check `system.query_log`.
2.  Optimize `ORDER BY` keys in tables.
3.  Check if merges are lagging.

## Disaster Recovery
- **Replay**: Use `events.raw.v1` (7 days) to re-process headers.
- **Deep Replay**: Restore from Iceberg (S3) if data older than 7 days is needed.
