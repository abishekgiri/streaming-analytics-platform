package com.platform.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.avro.generic.GenericRecord;
import java.time.Duration;
import java.sql.Timestamp;

import org.apache.flink.configuration.Configuration;

public class StreamingJob {

        public static void main(String[] args) throws Exception {
                Configuration config = new Configuration();
                config.setString("s3.endpoint", "http://minio:9000");
                config.setString("s3.access-key", "minioadmin");
                config.setString("s3.secret-key", "minioadmin");
                config.setString("s3.path.style.access", "true");

                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);

                // Checkpointing every 60 seconds (Production tuning: balances recovery time vs.
                // small file overhead)
                env.enableCheckpointing(60000);
                env.getCheckpointConfig().setCheckpointStorage("s3://checkpoints/flink-agg-v1");

                String bootstrapServers = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "broker:29092");
                String schemaRegistryUrl = System.getenv().getOrDefault("SCHEMA_REGISTRY_URL",
                                "http://schema-registry:8081");
                String topic = "events.raw.v1";
                String groupId = "flink-group-agg-v1";

                // Embedding schema to avoid file path issues in container
                String schemaString = "{" +
                                "  \"namespace\": \"com.platform.events\"," +
                                "  \"type\": \"record\"," +
                                "  \"name\": \"ClickstreamEvent\"," +
                                "  \"fields\": [" +
                                "    {\"name\": \"event_id\", \"type\": \"string\"}," +
                                "    {\"name\": \"user_id\", \"type\": \"string\"}," +
                                "    {\"name\": \"session_id\", \"type\": \"string\"}," +
                                "    {\"name\": \"event_type\", \"type\": \"string\"}," +
                                "    {\"name\": \"event_ts\", \"type\": \"long\", \"logicalType\": \"timestamp-millis\"},"
                                +
                                "    {\"name\": \"ingest_ts\", \"type\": \"long\", \"logicalType\": \"timestamp-millis\"},"
                                +
                                "    {\"name\": \"source\", \"type\": \"string\", \"default\": \"unknown\"}," +
                                "    {\"name\": \"campaign_id\", \"type\": [\"null\", \"string\"], \"default\": null},"
                                +
                                "    {\"name\": \"device\", \"type\": \"string\", \"default\": \"unknown\"}," +
                                "    {\"name\": \"geo\", \"type\": \"string\", \"default\": \"unknown\"}," +
                                "    {\"name\": \"schema_version\", \"type\": \"string\", \"default\": \"v1\"}" +
                                "  ]" +
                                "}";

                org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(schemaString);

                KafkaSource<GenericRecord> source = KafkaSource.<GenericRecord>builder()
                                .setBootstrapServers(bootstrapServers)
                                .setTopics(topic)
                                .setGroupId(groupId)
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setValueOnlyDeserializer(ConfluentRegistryAvroDeserializationSchema.forGeneric(
                                                schema,
                                                schemaRegistryUrl))
                                .build();

                DataStream<GenericRecord> stream = env.fromSource(source,
                                WatermarkStrategy.<GenericRecord>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                                .withTimestampAssigner(
                                                                (event, timestamp) -> (Long) event.get("event_ts")),
                                "Kafka Source");

                final org.apache.flink.util.OutputTag<Tuple6<String, String, String, String, Long, Integer>> lateEventsTag = new org.apache.flink.util.OutputTag<Tuple6<String, String, String, String, Long, Integer>>(
                                "late-events") {
                };

                // Aggregation: (window_start, window_end, campaign, geo, device, count)
                // Note: For revenue we would sum price. Since we don't have price, let's just
                // count events.
                org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<Tuple6<Timestamp, Timestamp, String, String, String, Long>> aggregated = stream
                                .map(new MapFunction<GenericRecord, Tuple6<String, String, String, String, Long, Integer>>() {
                                        @Override
                                        public Tuple6<String, String, String, String, Long, Integer> map(
                                                        GenericRecord value) throws Exception {
                                                String campaign = value.get("campaign_id") != null
                                                                ? value.get("campaign_id").toString()
                                                                : "organic";
                                                String geo = value.get("geo") != null ? value.get("geo").toString()
                                                                : "unknown";
                                                String device = value.get("device") != null
                                                                ? value.get("device").toString()
                                                                : "unknown";
                                                return Tuple6.of(campaign, geo, device, "dummy", 1L, 0); // extra fields
                                                                                                         // to match key
                                                                                                         // selector if
                                                                                                         // needed,
                                                                                                         // simple tuple
                                                                                                         // here
                                        }
                                })
                                // KeyBy Campaign, Geo, Device. Tuple fields are 0, 1, 2
                                .keyBy(t -> t.f0 + "|" + t.f1 + "|" + t.f2)
                                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                                .sideOutputLateData(lateEventsTag)
                                .apply(new WindowFunction<Tuple6<String, String, String, String, Long, Integer>, Tuple6<Timestamp, Timestamp, String, String, String, Long>, String, TimeWindow>() {
                                        @Override
                                        public void apply(String key, TimeWindow window,
                                                        Iterable<Tuple6<String, String, String, String, Long, Integer>> input,
                                                        Collector<Tuple6<Timestamp, Timestamp, String, String, String, Long>> out) {
                                                long count = 0;
                                                String campaign = "";
                                                String geo = "";
                                                String device = "";

                                                for (Tuple6<String, String, String, String, Long, Integer> in : input) {
                                                        count++;
                                                        campaign = in.f0;
                                                        geo = in.f1;
                                                        device = in.f2;
                                                }

                                                out.collect(Tuple6.of(
                                                                new Timestamp(window.getStart()),
                                                                new Timestamp(window.getEnd()),
                                                                campaign,
                                                                geo,
                                                                device,
                                                                count));
                                        }
                                });

                // -------------------------------------------------------------
                // DLQ Sink (Late Events)
                // -------------------------------------------------------------
                DataStream<Tuple6<String, String, String, String, Long, Integer>> lateStream = aggregated
                                .getSideOutput(lateEventsTag);

                // Convert Tuple to String for simple DLQ sink
                lateStream.map(t -> "LATE: " + t.toString())
                                .sinkTo(org.apache.flink.connector.kafka.sink.KafkaSink.<String>builder()
                                                .setBootstrapServers(bootstrapServers)
                                                .setRecordSerializer(
                                                                org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
                                                                                .builder()
                                                                                .setTopic("events.late.v1")
                                                                                .setValueSerializationSchema(
                                                                                                new org.apache.flink.api.common.serialization.SimpleStringSchema())
                                                                                .build())
                                                .build());

                // Sink to ClickHouse
                aggregated.addSink(JdbcSink.sink(
                                "INSERT INTO stream_analytics.metrics_1m (window_start, window_end, campaign_id, geo, device, event_count, revenue) VALUES (?, ?, ?, ?, ?, ?, 0.0)",
                                (statement, event) -> {
                                        statement.setTimestamp(1, event.f0);
                                        statement.setTimestamp(2, event.f1);
                                        statement.setString(3, event.f2);
                                        statement.setString(4, event.f3);
                                        statement.setString(5, event.f4);
                                        statement.setLong(6, event.f5);
                                },
                                JdbcExecutionOptions.builder()
                                                .withBatchSize(100)
                                                .withBatchIntervalMs(200)
                                                .withMaxRetries(5)
                                                .build(),
                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                                .withUrl("jdbc:clickhouse://clickhouse:8123/stream_analytics")
                                                // clickhouse-jdbc often does not require user/pass by default in dev,
                                                // or 'default'/''
                                                .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                                                .withUsername("flink")
                                                .withPassword("flink")
                                                .build()));

                // -------------------------------------------------------------
                // Iceberg Sink (Cold Path)
                // -------------------------------------------------------------

                // Hadoop Conf for MinIO
                org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
                hadoopConf.set("fs.s3a.endpoint", "http://minio:9000");
                hadoopConf.set("fs.s3a.access.key", "minioadmin");
                hadoopConf.set("fs.s3a.secret.key", "minioadmin");
                hadoopConf.set("fs.s3a.path.style.access", "true");
                hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

                String warehouseLocation = "s3a://warehouse/";
                java.util.Map<String, String> catalogProps = new java.util.HashMap<>();
                catalogProps.put("warehouse", warehouseLocation);

                org.apache.iceberg.flink.CatalogLoader catalogLoader = org.apache.iceberg.flink.CatalogLoader
                                .hadoop("minio_catalog", hadoopConf, catalogProps);

                // Define Schema
                org.apache.iceberg.Schema icebergSchema = new org.apache.iceberg.Schema(
                                org.apache.iceberg.types.Types.NestedField.required(1, "event_id",
                                                org.apache.iceberg.types.Types.StringType.get()),
                                org.apache.iceberg.types.Types.NestedField.required(2, "user_id",
                                                org.apache.iceberg.types.Types.StringType.get()),
                                org.apache.iceberg.types.Types.NestedField.required(3, "event_type",
                                                org.apache.iceberg.types.Types.StringType.get()),
                                org.apache.iceberg.types.Types.NestedField.required(4, "event_ts",
                                                org.apache.iceberg.types.Types.TimestampType.withZone()),
                                org.apache.iceberg.types.Types.NestedField.optional(5, "campaign_id",
                                                org.apache.iceberg.types.Types.StringType.get()),
                                org.apache.iceberg.types.Types.NestedField.required(6, "geo",
                                                org.apache.iceberg.types.Types.StringType.get()),
                                org.apache.iceberg.types.Types.NestedField.required(7, "device",
                                                org.apache.iceberg.types.Types.StringType.get()));

                org.apache.iceberg.catalog.TableIdentifier tableId = org.apache.iceberg.catalog.TableIdentifier
                                .of("iceberg_events");

                // Create table if not exists
                org.apache.iceberg.catalog.Catalog catalog = catalogLoader.loadCatalog();
                if (!catalog.tableExists(tableId)) {
                        // Partition by day (using event_ts)
                        org.apache.iceberg.PartitionSpec spec = org.apache.iceberg.PartitionSpec
                                        .builderFor(icebergSchema)
                                        .day("event_ts")
                                        .build();

                        // Table properties for production performance
                        java.util.Map<String, String> props = new java.util.HashMap<>();
                        props.put("write.format.default", "parquet");
                        props.put("write.target-file-size-bytes", "134217728"); // 128 MB target set, though demo volume
                                                                                // is low
                        props.put("write.distribution-mode", "hash"); // Prevent data skew

                        catalog.createTable(tableId, icebergSchema, spec, props);
                }

                // Convert Stream to RowData and Sink
                DataStream<org.apache.flink.table.data.RowData> rowStream = stream
                                .map(new MapFunction<GenericRecord, org.apache.flink.table.data.RowData>() {
                                        @Override
                                        public org.apache.flink.table.data.RowData map(GenericRecord value)
                                                        throws Exception {
                                                org.apache.flink.table.data.GenericRowData row = new org.apache.flink.table.data.GenericRowData(
                                                                7);
                                                row.setField(0, org.apache.flink.table.data.StringData
                                                                .fromString(value.get("event_id").toString()));
                                                row.setField(1, org.apache.flink.table.data.StringData
                                                                .fromString(value.get("user_id").toString()));
                                                row.setField(2, org.apache.flink.table.data.StringData
                                                                .fromString(value.get("event_type").toString()));

                                                // Timestamp handling: Avro long (millis) -> Flink TimestampData
                                                long ts = (long) value.get("event_ts");
                                                row.setField(3, org.apache.flink.table.data.TimestampData
                                                                .fromEpochMillis(ts));

                                                Object camp = value.get("campaign_id");
                                                if (camp != null) {
                                                        row.setField(4, org.apache.flink.table.data.StringData
                                                                        .fromString(camp.toString()));
                                                } else {
                                                        row.setField(4, null);
                                                }

                                                row.setField(5, org.apache.flink.table.data.StringData
                                                                .fromString(value.get("geo").toString()));
                                                row.setField(6, org.apache.flink.table.data.StringData
                                                                .fromString(value.get("device").toString()));

                                                return row;
                                        }
                                });

                org.apache.iceberg.flink.sink.FlinkSink.forRowData(rowStream)
                                .tableLoader(org.apache.iceberg.flink.TableLoader.fromCatalog(catalogLoader, tableId))
                                .append();

                env.execute("Flink Realtime Aggregation V1");
        }
}
