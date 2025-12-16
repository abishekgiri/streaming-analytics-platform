
import time
import os
import uuid
import json
import struct
import io
import requests
import avro.schema
import avro.io
from kafka import KafkaProducer

# Logging setup
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
TOPIC = "events.raw.v1"
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "broker:29092")
SUBJECT = f"{TOPIC}-value"

# Schema Definition (identical to main.py)
SCHEMA_DICT = {
  "namespace": "com.platform.events",
  "type": "record",
  "name": "ClickstreamEvent",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "user_id", "type": "string"},
    {"name": "session_id", "type": "string"},
    {"name": "event_type", "type": "string"},
    {"name": "event_ts", "type": "long", "logicalType": "timestamp-millis"},
    {"name": "ingest_ts", "type": "long", "logicalType": "timestamp-millis"},
    {"name": "source", "type": "string", "default": "unknown"},
    {"name": "campaign_id", "type": ["null", "string"], "default": None},
    {"name": "device", "type": "string", "default": "unknown"},
    {"name": "geo", "type": "string", "default": "unknown"},
    {"name": "schema_version", "type": "string", "default": "v1"}
  ]
}

def register_schema():
    """Register schema and return schema ID."""
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{SUBJECT}/versions"
    payload = {"schema": json.dumps(SCHEMA_DICT)}
    headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        schema_id = response.json()["id"]
        logger.info(f"Schema registered with ID: {schema_id}")
        return schema_id
    except Exception as e:
        logger.error(f"Failed to register schema: {e}")
        # Assuming schema already exists from main generator, try to fetch specific version or just hardcode ID if known?
        # Better: try request again or assume id 1 for stable env
        # But failing here is fatal.
        raise

class AvroSerializer:
    def __init__(self, schema_id, schema_dict):
        self.schema_id = schema_id
        self.schema = avro.schema.parse(json.dumps(schema_dict))

    def serialize(self, record):
        with io.BytesIO() as out:
            out.write(struct.pack('>bI', 0, self.schema_id))
            encoder = avro.io.BinaryEncoder(out)
            writer = avro.io.DatumWriter(self.schema)
            writer.write(record, encoder)
            return out.getvalue()

def produce_verification_events():
    logger.info("Starting verification event generation...")
    schema_id = register_schema()
    serializer = AvroSerializer(schema_id, SCHEMA_DICT)
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)

    now_ms = int(time.time() * 1000)

    # 1. Advance Watermark (send events 2 minutes in future)
    logger.info("Step 1: Advancing Watermark to T+120s")
    for i in range(200):
        future_ts = now_ms + 120000 + (i * 1000)
        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": "user_future",
            "session_id": "sess_future",
            "event_type": "view",
            "event_ts": future_ts,
            "ingest_ts": now_ms,
            "source": "verification_script",
            "campaign_id": "camp_future",
            "device": "mobile",
            "geo": "US",
            "schema_version": "v1"
        }
        key = event['user_id'].encode('utf-8')
        value = serializer.serialize(event)
        producer.send(TOPIC, key=key, value=value)
    
    producer.flush()
    time.sleep(5) # Give Flink time to process watermark

    # 2. Inject Late Events (send events 5 minutes in PAST)
    # Window size is 1 min. Allowed lateness is 5s (in WatermarkStrategy). 
    # With watermark around T+120, a T-300 event is definitely late.
    logger.info("Step 2: Injecting LATE events (T-300s)")
    for i in range(5):
        late_ts = now_ms - 300000 + (i * 1000)
        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": "user_verify_late",
            "session_id": "sess_late",
            "event_type": "view",
            "event_ts": late_ts,
            "ingest_ts": now_ms,
            "source": "verification_script",
            "campaign_id": "camp_verify_dropped",
            "device": "mobile",
            "geo": "US",
            "schema_version": "v1"
        }
        key = event['user_id'].encode('utf-8')
        value = serializer.serialize(event)
        producer.send(TOPIC, key=key, value=value)
        logger.info(f"Sent late event: {event['event_id']}")

    producer.flush()
    producer.close()
    logger.info("Verification generation complete.")

if __name__ == "__main__":
    produce_verification_events()
