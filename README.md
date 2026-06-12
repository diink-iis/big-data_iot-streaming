# Заключительный проект — IoT Streaming Pipeline (Apache Flink)

## Задача

Реализовать потоковый пайплайн обработки данных с IoT-устройств:
- генератор событий → Kafka
- обогащение справочником типов из PostgreSQL
- агрегация в 1-минутных окнах по event time (средняя температура, медиана влажности)
- результат → Kafka

---

## Архитектура

```
Kafka (iot-events)
    │
    ▼  [DataStream API] — KafkaSource, parse JSON
    │
    ▼  [Переход #1: DataStream → Table API]
    │  from_data_stream() + WATERMARK (event time -5s)
    │
    ▼  [Table API / SQL DDL] — CREATE TABLE device_types (JDBC → PostgreSQL)
    │
    ▼  [Table API SQL] — LEFT JOIN FOR SYSTEM_TIME AS OF (temporal lookup join)
    │
    ▼  [Переход #2: Table → DataStream API]
    │  to_data_stream() — watermarks сохраняются
    │
    ▼  [DataStream API] — key_by(type_id)
    │  TumblingEventTimeWindows(1 минута)
    │  ProcessWindowFunction: avg(temperature) + median(humidity)
    │
    ▼  [Переход #3: DataStream → Table API]
    │  from_data_stream()
    │
    ▼  [Table API / SQL DDL] — CREATE TABLE iot_aggregated (Kafka sink)
    │
    ▼  execute_insert("iot_aggregated")
    │
Kafka (iot-aggregated)
{"window_time":"14:23","type_name":"Multi-Sensor","avg_temperature":27.41,"median_humidity":62.5}
```

---

## Файлы проекта

| Файл | Описание |
|------|----------|
| `docker-compose.yml` | Весь стек: Kafka, ZooKeeper, PostgreSQL, Flink (JM+TM), генератор, job |
| `generator/producer.py` | Генератор IoT событий, 1 сообщение/сек, публикует в `iot-events` |
| `sql/ddl.sql` | `CREATE TABLE device_types (id, type_name)` |
| `sql/dml.sql` | `INSERT` — 5 типов устройств |
| `flink_job/job.py` | Flink-задание (PyFlink 1.18): DataStream + Table API, переходы, median |
| `flink_job/Dockerfile` | flink:1.18 + Python 3 + Kafka/JDBC/PostgreSQL JARs |
| `consumer.py` | Утилита для просмотра результатов из `iot-aggregated` |

---

## Формат сообщений

**Входной топик** `iot-events`:
```json
{
  "device_id":   "device_3_002",
  "type_id":     3,
  "event_time":  "2024-01-15T14:23:01.000Z",
  "temperature": 27.45,
  "humidity":    61.2
}
```

**Выходной топик** `iot-aggregated` (раз в минуту по типу устройства):
```json
{
  "window_time":     "14:23",
  "type_name":       "Multi-Sensor",
  "avg_temperature": 27.41,
  "median_humidity": 62.5
}
```

---

## Запуск

```bash
# 1. Поднять весь стек
docker compose up --build

# 2. Flink Web UI (мониторинг задания)
open http://localhost:8081

# 3. Просмотр результатов (на хосте, после pip install kafka-python)
KAFKA_BOOTSTRAP_SERVERS=localhost:29092 python consumer.py

# Остановить всё
docker compose down
```

> **macOS (Apple Silicon).** Образ собирается нативно под ARM64 — эмуляция x86 не нужна.
> При сборке ставится `openjdk-11-jdk-headless`: PyFlink-мост `pemja` компилируется из
> исходников и требует заголовков JDK (в базовом образе только JRE). Подходит любой
> Docker-движок (Docker Desktop или Colima: `brew install colima docker docker-compose && colima start`).

---

## Проверено (end-to-end запуск)

Пайплайн запущен и проверен полностью. Пример реального вывода из топика `iot-aggregated`:

```json
{"window_time":"20:51","type_name":"CO2 Sensor","avg_temperature":24.17,"median_humidity":54.48}
{"window_time":"20:51","type_name":"Humidity Sensor","avg_temperature":27.06,"median_humidity":72.99}
{"window_time":"20:51","type_name":"Temperature Sensor","avg_temperature":29.14,"median_humidity":53.3}
{"window_time":"20:51","type_name":"Pressure Sensor","avg_temperature":30.97,"median_humidity":33.25}
{"window_time":"20:51","type_name":"Multi-Sensor","avg_temperature":27.96,"median_humidity":54.74}
```

Подтверждено: генерация → Kafka → lookup join с PostgreSQL (`type_name` подставляется
из справочника) → 1-минутное окно по event time → avg(temp) + median(humidity) → Kafka sink.
Окна закрываются каждую минуту, все 5 типов устройств присутствуют.

### Ключевые технические решения

| Узел | Решение | Почему |
|------|---------|--------|
| Сборка PyFlink на ARM | `openjdk-11-jdk-headless` + сборка `pemja` из исходников | на linux/aarch64 нет готового wheel; нужны заголовки JDK |
| Lookup join со справочником | `FOR SYSTEM_TIME AS OF e.proc_time` (а не `event_time`) | JDBC-таблица — processing-time lookup, не versioned-таблица |
| Watermark / event-time окна | `parallelism = 1` | при большем параллелизме простаивающий субтаск держит watermark на `Long.MIN`, окна не закрываются |
| Window-функция | `process(self, key, ctx, elements)` + `yield` | в PyFlink нет `out`-коллектора (в отличие от Java API) |

---

## Реализованные требования

| Требование | Реализация |
|-----------|-----------|
| Генератор IoT (1/сек, device_id/type_id/event_time/temp/humidity) | `generator/producer.py` |
| DDL/DML скрипты для справочника типов | `sql/ddl.sql`, `sql/dml.sql` |
| Source из Kafka | DataStream API — `KafkaSource` |
| Source из PostgreSQL (справочник) | Table API DDL — JDBC connector, processing-time lookup join |
| JOIN событий со справочником | SQL: `LEFT JOIN … FOR SYSTEM_TIME AS OF e.proc_time` |
| Event time + watermark | Schema с `WATERMARK … - INTERVAL '5' SECOND` |
| Tumbling window 1 минута | `TumblingEventTimeWindows.of(Time.minutes(1))` |
| Средняя температура | `sum / len` в `ProcessWindowFunction` |
| Медиана влажности | `statistics.median()` в `ProcessWindowFunction` |
| Sink в Kafka (time/type/avg_temp/median_hum) | Table API DDL — Kafka connector |
| Source/sink на SQL/Table API | PostgreSQL (source) + Kafka (sink) через DDL |
| Переход DataStream ↔ Table API | 3 перехода: DataStream→Table→DataStream→Table |

---

## Зависимости (Flink-кластер)

- Apache Flink 1.18.0
- `flink-sql-connector-kafka:3.1.0-1.18`
- `flink-connector-jdbc:3.1.2-1.18`
- `postgresql:42.7.1`
- PyFlink 1.18.0
