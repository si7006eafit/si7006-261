    CREATE STREAM sensor_events_raw (
    sensor_id VARCHAR KEY,
    tipo VARCHAR,
    valor DOUBLE,
    unidad VARCHAR,
    timestamp VARCHAR
    ) WITH (
    KAFKA_TOPIC='sensor_events_raw',
    VALUE_FORMAT='JSON'
    );

    CREATE STREAM sensor_events_raw_mongo
    WITH (
    KAFKA_TOPIC='sensor_events_raw_mongo',
    VALUE_FORMAT='JSON'
    ) AS
    SELECT
    sensor_id AS sensor_id_key,
    AS_VALUE(sensor_id) AS sensor_id,
    tipo,
    valor,
    unidad,
    timestamp
    FROM sensor_events_raw
    EMIT CHANGES;

    CREATE STREAM sensor_alerts
    WITH (
    KAFKA_TOPIC='sensor_alerts',
    VALUE_FORMAT='JSON'
    ) AS
    SELECT
    sensor_id,
    tipo,
    valor,
    unidad,
    timestamp,
    'ALTA_TEMPERATURA' AS alerta
    FROM sensor_events_raw
    WHERE tipo = 'temperatura' AND valor >= 30.0
    EMIT CHANGES;

