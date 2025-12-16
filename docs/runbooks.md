# Operational Runbooks

## 1. Alert Response

### High Consumer Lag
**Trigger**: Kafka Consumer Lag > 10,000 events.
**Impact**: Delayed insights in ClickHouse.
**Diagnosis**:
1. Check Flink Backpressure via Grafana.
   - If High (1.0): Downstream (ClickHouse/Iceberg) is slow.
   - If Low (0.0): Flink is CPU bound or source is empty.
2. Check ClickHouse Logs for insert errors.
**Mitigation**:
- If ClickHouse is slow: Optimize table merge/parts.
- If Flink is slow: Scale TaskManagers (`docker-compose scale taskmanager=2`).

### Job Restart / Crash
**Trigger**: `fullRestarts` > 0.
**Action**:
1. Check JobManager logs: `docker logs docker-compose-jobmanager-1`.
2. Identify if transient (OOM) or persistent (Code bug).
3. If OOM, increase TaskManager memory.

## 2. Disaster Recovery

### Restore from S3 Checkpoint
If Flink state is corrupted or cluster is lost:
1. Identify last valid checkpoint path in MinIO (`s3://checkpoints/UUID/chk-X`).
2. Submit job with allow-non-restored-state if schema changed, or standard restore:
```bash
flink run --fromSavepoint s3://checkpoints/... -d job.jar
```
**Note**: Our setup has checkpointing enabled. Flink usually auto-recovers. This is for manual intervention.

## 3. DLQ Replay
**Scenario**: Late events accumulated in `events.late.v1` and need to be re-processed (e.g. business logic changed to accept them).
**Procedure**:
1. Identify the topic `events.late.v1`.
2. Use a replay job (or netcat for simple text) to pipe back to `events.raw.v1` if viable, OR ingest into a separate partial table.
   *Currently manual replay via Kafka generic tools is recommended.*

## 4. Maintenance
### Upgrading Flink
1. Stop job with Savepoint.
2. Update Docker Image tag.
3. Restart cluster.
4. Restore from Savepoint.
