# Architecture

## Overview
The system allows for real-time ingestion, processing, and analytics of event data.

## Data Flow
1.  **Producers**: Applications (or the `event-gen` tool) send Avro-encoded events to Kafka `events.raw.v1`.
2.  **Streaming Core**: Flink jobs consume from Kafka.
    - Validate schema against Schema Registry.
    - Assign watermarks based on `event_ts`.
    - Group by `user_id` or `session_id` for stateful aggregations.
3.  **Hot Path**: Aggregated metrics are written to ClickHouse for low-latency dashboards (Grafana).
    - Windowed counts (1 min, 5 min).
    - Session summaries.
4.  **Cold Path**: Raw events (and optionally aggregates) are written to Iceberg (on MinIO/S3) for historical analysis, replay, and audit.

## Components
- **Kafka**: Message bus / event log.
- **Schema Registry**: Enforces data contracts.
- **Flink**: Stateful stream processing engine.
- **ClickHouse**: Real-time OLAP database.
- **Iceberg + MinIO**: Data lakehouse storage.
