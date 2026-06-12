"""
Kafka consumer для проверки выходного топика iot-aggregated.
Запуск: python consumer.py
"""

import json
import os
from kafka import KafkaConsumer

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC = "iot-aggregated"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_SERVERS,
    auto_offset_reset="earliest",
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    group_id="demo-consumer",
)

print(f"Читаем из '{TOPIC}'...\n{'─'*55}")
print(f"{'Время':<8} {'Тип устройства':<22} {'Ср.темп':>8} {'Мед.влаж':>10}")
print("─" * 55)

for msg in consumer:
    r = msg.value
    print(
        f"{r.get('window_time','?'):<8} "
        f"{r.get('type_name','?'):<22} "
        f"{r.get('avg_temperature', 0):>8.2f} "
        f"{r.get('median_humidity', 0):>10.2f}"
    )
