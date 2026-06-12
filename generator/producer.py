"""
IoT message generator — publishes one event per second to Kafka.

Message format:
{
    "device_id":   "device_3_002",
    "type_id":     3,
    "event_time":  "2024-01-15T12:00:01.000Z",
    "temperature": 23.45,
    "humidity":    61.2
}
"""

import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC", "iot-events")
INTERVAL      = 1.0 / int(os.getenv("EVENTS_PER_SECOND", "1"))

# type_id → number of devices of that type
DEVICE_COUNTS = {1: 5, 2: 4, 3: 6, 4: 3, 5: 2}

# Realistic sensor ranges per type
SENSOR_RANGES = {
    1: {"temp": (15.0, 45.0), "hum": (20.0, 80.0)},   # Temperature Sensor
    2: {"temp": (18.0, 35.0), "hum": (40.0, 95.0)},   # Humidity Sensor
    3: {"temp": (10.0, 50.0), "hum": (30.0, 90.0)},   # Multi-Sensor
    4: {"temp": (20.0, 40.0), "hum": (25.0, 75.0)},   # Pressure Sensor
    5: {"temp": (18.0, 28.0), "hum": (35.0, 70.0)},   # CO2 Sensor
}


def make_event() -> dict:
    type_id = random.choice(list(DEVICE_COUNTS.keys()))
    device_num = random.randint(1, DEVICE_COUNTS[type_id])
    ranges = SENSOR_RANGES[type_id]
    now = datetime.now(timezone.utc)               # единый timestamp на всё событие
    return {
        "device_id":  f"device_{type_id}_{device_num:03d}",
        "type_id":    type_id,
        "event_time": now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z",
        "temperature": round(random.uniform(*ranges["temp"]), 2),
        "humidity":    round(random.uniform(*ranges["hum"]), 2),
    }


def connect(retries: int = 20) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            print(f"[generator] Connected to {KAFKA_SERVERS}")
            return producer
        except NoBrokersAvailable:
            print(f"[generator] Kafka not ready, attempt {attempt}/{retries}, retrying in 3s...")
            time.sleep(3)
    raise RuntimeError("Could not connect to Kafka after multiple retries")


def main() -> None:
    producer = connect()
    print(f"[generator] Publishing to topic '{KAFKA_TOPIC}' every {INTERVAL:.1f}s")
    while True:
        event = make_event()
        producer.send(KAFKA_TOPIC, value=event)
        print(f"[generator] → {event}")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
