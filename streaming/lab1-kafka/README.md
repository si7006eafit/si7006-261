# En directorio lab1-kafka
# subir el docker [docker-compose.yml](docker-compose.yml)

    docker compose up -d

# crear topico

    docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --topic sensor_events_raw \
    --partitions 1 \
    --replication-factor 1

    docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --list

# Crear el stream y el procesamiento en ksqlDB [db.sql](db.sql)

    docker exec -it ksqldb-server ksql http://localhost:8088

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

# Configurar el sink hacia MongoDB: [mongo-sink.json](mongo-sink.json)

    {
    "name": "mongo-sink-alerts",
    "config": {
        "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
        "tasks.max": "1",
        "topics": "sensor_alerts",
        "connection.uri": "mongodb://mongodb:27017",
        "database": "streaming_demo",
        "collection": "alertas_temperatura",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneBusinessKeyStrategy",
        "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.PartialValueStrategy",
        "document.id.strategy.partial.value.projection.type": "AllowList",
        "document.id.strategy.partial.value.projection.list": "sensor_id,timestamp"
    }
    }

    curl -X POST http://localhost:8083/connectors \
    -H "Content-Type: application/json" \
    -d @mongo-sink.json

# grabar datos crudos a mongodb: [mongo-sink-raw.json](mongo-sink-raw.json)

    {
    "name": "mongo-sink-raw",
    "config": {
        "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
        "tasks.max": "1",
        "topics": "sensor_events_raw_mongo",

        "connection.uri": "mongodb://mongodb:27017",
        "database": "streaming_demo",
        "collection": "eventos_raw",

        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",

        "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.InsertOneDefaultStrategy"
    }
    }

    curl -X POST http://localhost:8083/connectors \
    -H "Content-Type: application/json" \
    -d @mongo-sink-raw.json


# instalar dependencias y ejecucón del productor

    python -m pip install confluent-kafka

    python producer.py

# ver datos en mongodb

    docker exec -it mongodb mongosh

    use streaming_demo
    db.alertas_temperatura.find().pretty()


# subir grafana

    cd lab1-grafana
    docker compose up -d

# DIAGNÓSTICO

## 1 Mongo tiene datos?

    docker exec -it mongodb mongosh --eval "db.alertas_temperatura.countDocuments()"

## 2 API responde?

    curl http://localhost:8000/alerts_summary

## 3 API timeseries?

    curl http://localhost:8000/alerts_timeseries

## 4 Grafana ve la API?

    docker exec -it grafana curl http://dashboard-api:8000/alerts_summary

# abra un navegador para acceder a grafana:

[http://localhost:3000](http://localhost:3000)

