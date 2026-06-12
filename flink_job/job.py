"""
IoT Flink Streaming Job
=======================
Архитектура (event time):

  Kafka (iot-events)
      │  [1] DataStream API: KafkaSource + parse JSON
      │
      ▼  [2] DataStream → Table API  (переход #1)
  from_data_stream() со схемой + WATERMARK
      │
      │  [3] Table API SQL DDL: device_types (JDBC PostgreSQL lookup)
      │
      ▼  [4] Table API SQL: LEFT JOIN FOR SYSTEM_TIME AS OF (temporal lookup join)
  enriched_table
      │
      ▼  [5] Table → DataStream API  (переход #2)
  to_data_stream()
      │
      │  [6] DataStream: key_by(type_id)
      │      TumblingEventTimeWindows(1 minute)
      │      ProcessWindowFunction → avg(temp) + median(humidity)
      │
      ▼  [7] DataStream → Table API  (переход #3)
  from_data_stream()
      │
      │  [8] Table API SQL DDL: iot_aggregated (Kafka sink)
      │
      ▼
  Kafka (iot-aggregated): {time, type_name, avg_temperature, median_humidity}
"""

import json
import os
import statistics
from datetime import datetime, timezone
from typing import Iterable

from pyflink.common import Row, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.table import DataTypes, Schema, StreamTableEnvironment

# ── Configuration (via env vars from docker-compose) ────────────────────────
KAFKA_SERVERS     = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
POSTGRES_URL      = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/iot_db")
POSTGRES_USER     = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

# Watermark tolerance: допускаем опоздание событий до 5 секунд
WATERMARK_LAG_SEC = 5


# ── Window function: avg(temperature) + median(humidity) ────────────────────

class IoTWindowFunction(ProcessWindowFunction):
    """
    Обрабатывает все события в 1-минутном окне для одного type_id.

    Поля входного Row (порядок из enriched_table SELECT):
        0: device_id  (str)
        1: type_id    (int)
        2: temperature (float)
        3: humidity   (float)
        4: event_time (datetime / Instant)
        5: type_name  (str)
    """

    def process(
        self,
        key: int,                          # type_id (ключ key_by)
        context: "ProcessWindowFunction.Context",
        elements: Iterable[Row],
    ) -> Iterable[Row]:
        # В PyFlink process() возвращает (yield) результаты, а не пишет в коллектор.
        type_name  = "Unknown"
        temps      = []
        humidities = []

        for row in elements:
            type_name = row[5]
            temps.append(float(row[2]))
            humidities.append(float(row[3]))

        if not temps:
            return

        avg_temp = sum(temps) / len(temps)
        median_h = statistics.median(humidities)   # O(n log n), точная медиана

        # Время начала окна → HH:MM (UTC)
        window_start_ms = context.window().start
        dt = datetime.fromtimestamp(window_start_ms / 1000.0, tz=timezone.utc)
        window_time = dt.strftime("%H:%M")

        yield Row(
            window_time,
            type_name,
            round(avg_temp, 2),
            round(median_h, 2),
        )


# ── Job entry point ──────────────────────────────────────────────────────────

def main() -> None:
    # ── [1] Среды выполнения ─────────────────────────────────────────────────
    env   = StreamExecutionEnvironment.get_execution_environment()
    # parallelism=1: единый генератор watermark покрывает все партиции Kafka.
    # При большем параллелизме «простаивающий» субтаск держит watermark окна на
    # Long.MIN_VALUE (итог = минимум по входам) и event-time окна не закрываются.
    env.set_parallelism(1)

    t_env = StreamTableEnvironment.create(env)

    # ── [2] DataStream API: источник Kafka ──────────────────────────────────
    #        Читаем сырые JSON-строки из топика iot-events
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_SERVERS)
        .set_topics("iot-events")
        .set_group_id("flink-iot-consumer")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Водяные знаки назначим через схему Table API — здесь отключаем
    raw_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source [iot-events]",
    )

    # ── [3] DataStream: парсинг JSON → типизированный Row ───────────────────
    PARSED_ROW_TYPE = Types.ROW_NAMED(
        ["device_id", "type_id", "temperature", "humidity", "event_time_ms"],
        [Types.STRING(), Types.INT(), Types.DOUBLE(), Types.DOUBLE(), Types.LONG()],
    )

    def parse_event(json_str: str) -> Row:
        data = json.loads(json_str)
        # event_time: "2024-01-15T12:00:01.000Z" → epoch milliseconds
        dt = datetime.strptime(data["event_time"], "%Y-%m-%dT%H:%M:%S.%fZ")
        event_ts_ms = int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
        return Row(
            str(data["device_id"]),
            int(data["type_id"]),
            float(data["temperature"]),
            float(data["humidity"]),
            event_ts_ms,
        )

    parsed_stream = raw_stream.map(parse_event, output_type=PARSED_ROW_TYPE)

    # ────────────────────────────────────────────────────────────────────────
    # ПЕРЕХОД #1: DataStream → Table API
    # Назначаем event time + watermark через схему таблицы
    # ────────────────────────────────────────────────────────────────────────
    iot_schema = (
        Schema.new_builder()
        .column("device_id",    DataTypes.STRING())
        .column("type_id",      DataTypes.INT())
        .column("temperature",  DataTypes.DOUBLE())
        .column("humidity",     DataTypes.DOUBLE())
        .column("event_time_ms", DataTypes.BIGINT())
        # Вычисляемый столбец: BIGINT (ms) → TIMESTAMP_LTZ(3) — event time для окна
        .column_by_expression(
            "event_time",
            f"TO_TIMESTAMP_LTZ(event_time_ms, 3)"
        )
        # Processing-time атрибут — нужен для lookup join со справочником JDBC
        .column_by_expression("proc_time", "PROCTIME()")
        # Watermark: допускаем опоздание до WATERMARK_LAG_SEC секунд
        .watermark(
            "event_time",
            f"event_time - INTERVAL '{WATERMARK_LAG_SEC}' SECOND"
        )
        .build()
    )

    iot_table = t_env.from_data_stream(parsed_stream, iot_schema)
    t_env.create_temporary_view("iot_events", iot_table)

    # ── [4] Table API SQL DDL: справочник устройств в PostgreSQL (lookup) ───
    #        connector = jdbc → Flink автоматически делает lookup join
    t_env.execute_sql(f"""
        CREATE TABLE device_types (
            id        INT,
            type_name STRING,
            PRIMARY KEY (id) NOT ENFORCED
        ) WITH (
            'connector'             = 'jdbc',
            'url'                   = '{POSTGRES_URL}',
            'table-name'            = 'device_types',
            'username'              = '{POSTGRES_USER}',
            'password'              = '{POSTGRES_PASSWORD}',
            'lookup.cache.max-rows' = '100',
            'lookup.cache.ttl'      = '600s'
        )
    """)

    # ── [5] Table API SQL: JOIN событий Kafka со справочником PostgreSQL ─────
    #        FOR SYSTEM_TIME AS OF e.proc_time — processing-time lookup join
    #        (обращение к JDBC-справочнику PG по ключу type_id = id)
    enriched_table = t_env.sql_query("""
        SELECT
            e.device_id,
            e.type_id,
            e.temperature,
            e.humidity,
            e.event_time,
            COALESCE(dt.type_name, 'Unknown') AS type_name
        FROM iot_events AS e
        LEFT JOIN device_types FOR SYSTEM_TIME AS OF e.proc_time AS dt
            ON e.type_id = dt.id
    """)

    # ────────────────────────────────────────────────────────────────────────
    # ПЕРЕХОД #2: Table API → DataStream
    # Watermark'и сохраняются и передаются в DataStream
    # ────────────────────────────────────────────────────────────────────────
    enriched_stream = t_env.to_data_stream(enriched_table)

    # ── [6] DataStream API: 1-минутное tumbling window по event time ─────────
    RESULT_ROW_TYPE = Types.ROW_NAMED(
        ["window_time", "type_name", "avg_temperature", "median_humidity"],
        [Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.DOUBLE()],
    )

    result_stream = (
        enriched_stream
        .key_by(lambda row: row[1], key_type=Types.INT())   # group by type_id
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .process(IoTWindowFunction(), output_type=RESULT_ROW_TYPE)
    )

    # Вывод в консоль для отладки (остаётся как DataStream sink)
    result_stream.print("WINDOW RESULT")

    # ────────────────────────────────────────────────────────────────────────
    # ПЕРЕХОД #3: DataStream → Table API (для записи в Kafka sink через SQL)
    # ────────────────────────────────────────────────────────────────────────
    result_schema = (
        Schema.new_builder()
        .column("window_time",     DataTypes.STRING())
        .column("type_name",       DataTypes.STRING())
        .column("avg_temperature", DataTypes.DOUBLE())
        .column("median_humidity", DataTypes.DOUBLE())
        .build()
    )
    result_table = t_env.from_data_stream(result_stream, result_schema)

    # ── [7] Table API SQL DDL: sink в Kafka (выходной топик) ────────────────
    t_env.execute_sql(f"""
        CREATE TABLE iot_aggregated (
            window_time     STRING,
            type_name       STRING,
            avg_temperature DOUBLE,
            median_humidity DOUBLE
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = 'iot-aggregated',
            'properties.bootstrap.servers' = '{KAFKA_SERVERS}',
            'format'                       = 'json'
        )
    """)

    # ── [8] Запуск задания ────────────────────────────────────────────────────
    # execute_insert отправляет граф выполнения на Flink-кластер.
    # Для streaming-задания НЕ вызываем .wait() — flink run сам управляет жизненным циклом.
    result_table.execute_insert("iot_aggregated")


if __name__ == "__main__":
    main()
