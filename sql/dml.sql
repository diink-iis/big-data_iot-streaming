-- DML: наполнение справочника типов IoT устройств
INSERT INTO device_types (id, type_name) VALUES
    (1, 'Temperature Sensor'),
    (2, 'Humidity Sensor'),
    (3, 'Multi-Sensor'),
    (4, 'Pressure Sensor'),
    (5, 'CO2 Sensor')
ON CONFLICT (id) DO NOTHING;
