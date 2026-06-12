-- DDL: справочник типов IoT устройств
CREATE TABLE IF NOT EXISTS device_types (
    id        INTEGER PRIMARY KEY,
    type_name VARCHAR(100) NOT NULL
);
