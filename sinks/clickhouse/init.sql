CREATE DATABASE IF NOT EXISTS stream_analytics;

USE stream_analytics;

-- Aggregated metrics table (Hot Path)
CREATE TABLE IF NOT EXISTS metrics_1m (
    window_start DateTime,
    window_end DateTime,
    campaign_id String,
    geo String,
    device String,
    event_count UInt64,
    revenue Float64
) ENGINE = ReplacingMergeTree()
ORDER BY (window_start, campaign_id, geo, device);

-- Optional: Materialized View for faster queries if needed, 
-- but for now we insert directly from Flink.
