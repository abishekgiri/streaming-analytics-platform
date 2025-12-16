# Data Contracts & Schema Registry

## Topics

### `events.raw.v1`
- **Purpose**: Immutable raw stream of clickstream events.
- **Partitions**: 6 (allow for some parallelism, can scale up).
- **Replication**: 3 (assuming 3 brokers in prod, 1 in dev).
- **Retention**: 7 days.
- **Key**: `user_id` (ensures user events are ordered).

### `events.dlq.v1`
- **Purpose**: Bad schema or poison messages.
- **Retention**: 30 days.

## Schema Policy
- **Compatibility**: `BACKWARD`
  - Can delete optional fields.
  - Can add optional fields.
  - Cannot delete required fields.
  - Cannot add required fields (must have default).
- **Validation**: Enforced by Confluent Schema Registry.

## Expected Load
- **Message Size**: ~0.5KB avg.
- **QPS**: Start at 100/sec, test up to 10k/sec.
