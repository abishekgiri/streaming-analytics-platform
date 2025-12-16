import time
import os
import uuid
import random
import click
import logging
import json
import struct
import io
import requests
import avro.schema
import avro.io
from kafka import KafkaProducer

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
TOPIC = "events.raw.v1"
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9094")
SUBJECT = f"{TOPIC}-value"

# Schema Definition (as dict for Registration)
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
        raise

class AvroSerializer:
    def __init__(self, schema_id, schema_dict):
        self.schema_id = schema_id
        # Parse schema for avro lib
        self.schema = avro.schema.parse(json.dumps(schema_dict))

    def serialize(self, record):
        """Serialize record with Confluent Wire Format: Magic Byte + Schema ID + Avro Data"""
        with io.BytesIO() as out:
            # Write Magic Byte (0) and Schema ID (4 bytes big-endian)
            out.write(struct.pack('>bI', 0, self.schema_id))
            
            # Write Avro data
            encoder = avro.io.BinaryEncoder(out)
            writer = avro.io.DatumWriter(self.schema)
            writer.write(record, encoder)
            
            return out.getvalue()

class EventGenerator:
    def __init__(self, mode, rate):
        self.mode = mode
        self.rate = rate
        self.users = [f"user_{i}" for i in range(100)]
        self.campaigns = [f"camp_{i}" for i in range(10)]
        self.event_types = ["view", "click", "purchase"]
        self.devices = ["mobile", "desktop", "tablet"]
        self.geos = ["US", "EU", "APAC"]

    def generate(self):
        user = random.choice(self.users)
        ts = int(time.time() * 1000)

        # Logic for 'shuffled' or 'late' events
        if self.mode == 'shuffled' and random.random() < 0.1:
            delay = random.randint(10000, 300000) 
            ts -= delay
            logger.info(f"Generating LATE event: {delay}ms late")

        return {
            "event_id": str(uuid.uuid4()),
            "user_id": user,
            "session_id": f"sess_{user}_{random.randint(1, 10)}",
            "event_type": random.choice(self.event_types),
            "event_ts": ts,
            "ingest_ts": int(time.time() * 1000),
            "source": "event-gen",
            "campaign_id": random.choice(self.campaigns) if random.random() > 0.5 else None,
            "device": random.choice(self.devices),
            "geo": random.choice(self.geos),
            "schema_version": "v1"
        }

@click.command()
@click.option('--rate', default=1.0, help='Events per second')
@click.option('--mode', type=click.Choice(['ordered', 'shuffled', 'duplicate']), default='ordered', help='Generation mode')
def produce(rate, mode):
    logger.info(f"Starting producer in {mode} mode at {rate} events/sec")
    
    # 1. Register Schema
    schema_id = register_schema()
    serializer = AvroSerializer(schema_id, SCHEMA_DICT)
    
    # 2. Setup Producer
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    
    generator = EventGenerator(mode, rate)
    delay = 1.0 / rate

    try:
        while True:
            event = generator.generate()
            
            # Serialize
            key = event['user_id'].encode('utf-8')
            value = serializer.serialize(event)

            # Duplicate logic
            count = 1
            if mode == 'duplicate' and random.random() < 0.1:
                count = 2
                logger.info(f"Generating DUPLICATE event {event['event_id']}")
            
            for _ in range(count):
                producer.send(TOPIC, key=key, value=value)
            
            producer.flush() # for low rate demo
            time.sleep(delay)
            
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.exception("Producer failure")
    finally:
        logger.info("Closing producer...")
        producer.close()

if __name__ == '__main__':
    produce()
