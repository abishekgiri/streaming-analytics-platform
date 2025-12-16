# Testing Plan

## Load Testing
**Tool**: `producers/event-gen` (Custom Python/Go tool).

### Scenarios
1.  **Baseline**: 100 events/sec. Verify < 500ms end-to-end latency.
2.  **Stress**: 10k events/sec. Verify linear scaling or identify bottleneck.
3.  **Soak**: Run for 24h to check memory leaks.

## Chaos Testing (Failure Injection)

### 1. Broker Failure
- **Action**: `docker stop broker`
- **Expected**: Producers buffer/block. Flink processing pauses.
- **Recovery**: `docker start broker` -> catchup burst.

### 2. TaskManager Failure
- **Action**: `docker stop taskmanager`
- **Expected**: Flink job restarts from last checkpoint. Duplicate writes handled by idempotency.

### 3. ClickHouse Outage
- **Action**: `docker stop clickhouse`
- **Expected**: Flink backpressures. Checkpoint times increase.
- **Recovery**: Start DB -> Flink resumes.
