# ejemplo: red social bluesky -> kafka -> terminal (print-out)

## descripción del caso:

Arquitectura:

Fuente: instancia red social: blue sky

Producer: Jetstream.

Bluesky expone un “firehose” WebSocket y la versión Jetstream ya viene en JSON y permite filtrar por colección (p. ej., app.bsky.feed.post). 

URI: wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post.

script Python que se conecta al streaming público y convierte cada evento a JSON con el esquema definido, luego publica a un topic de Kafka (social.posts).

Kafka cluster: una instancia de Kafka (single-broker + Zookeeper via docker-compose) para el lab.

topico de Kafka: social.posts

Consumers / downstream: consumidores que leen social.posts para analítica, indexación (Elasticsearch), RAG ingestion, dashboards, etc. En este caso: salida de mensajes a pantalla (print-out)


### docker-compose.yaml

    version: "3.8"
    services:
      zookeeper:
        image: confluentinc/cp-zookeeper:7.4.0
        environment:
          ZOOKEEPER_CLIENT_PORT: 2181
          ZOOKEEPER_TICK_TIME: 2000
        ports: ["2181:2181"]

      kafka:
        image: confluentinc/cp-kafka:7.4.0
        depends_on: [zookeeper]
        ports: ["9092:9092"]
        environment:
          KAFKA_BROKER_ID: 1
          KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
          KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
          KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

### subir el cluster kafka en docker:

    docker-compose up -d

### crear el topico social.posts

    docker exec -it lab3-kafka-1 \
      kafka-topics --create --topic social.posts \
      --bootstrap-server localhost:9092 \
      --partitions 3 --replication-factor 1


### consultar los tópicos:

    docker exec -it lab3-kafka-1 kafka-topics --bootstrap-server kafka:9092 --list

### JSON normalizado:

    {
      "id": "string",             // rkey del post
      "did": "string",            // DID del autor (repo)
      "created_at": "ISO8601",
      "text": "string",
      "langs": ["es","en"],
      "hashtags": ["string"],
      "mentions": ["@handle? opcional"],
      "uri": "string",            // at://did/app.bsky.feed.post/rkey (si disponible)
      "raw": {}                   // evento Jetstream completo (opcional)
    }

### Jetstream -> kafka

Dependencias:

    pip install websockets aiokafka orjson

Código python: jetstream_to_kafka.py

    import asyncio, json, re, os
    import orjson
    import websockets
    from aiokafka import AIOKafkaProducer

    JETSTREAM_URL = os.getenv(
        "JETSTREAM_URL",
        "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"
    )
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    TOPIC = os.getenv("TOPIC", "social.posts")

    hashtag_re = re.compile(r"(?:^|[\s])#([A-Za-z0-9_]+)")
    mention_re = re.compile(r"(?:^|[\s])@([A-Za-z0-9_.-]+)")

    def extract_tags_mentions(text: str):
        if not text:
            return [], []
        tags = [m.group(1) for m in hashtag_re.finditer(text)]
        mnts = [m.group(1) for m in mention_re.finditer(text)]
        # normaliza menciones a formato "@handle"
        mnts = [f"@{m}" for m in mnts]
        return tags, mnts

    def build_record(evt: dict):
        """
        Jetstream entrega JSON con campos como:
        {
          "kind":"commit",
          "time_us":"...",
          "commit":{
            "operation":"create",
            "collection":"app.bsky.feed.post",
            "rkey":"3lxy...",
            "record":{"$type":"app.bsky.feed.post","text":"...","createdAt":"...","langs":["es"]},
            "repo":"did:plc:...."   // DID del autor
          },
          "did":"did:plc:...."      // suele repetirse
        }
        """
        c = evt.get("commit") or {}
        rec = c.get("record") or {}
        if not rec:
            return None

        text = rec.get("text", "")
        tags, mnts = extract_tags_mentions(text)

        uri = None
        if c.get("collection") and c.get("rkey") and (evt.get("did") or c.get("repo")):
            uri = f"at://{evt.get('did', c.get('repo'))}/{c['collection']}/{c['rkey']}"

        out = {
            "id": c.get("rkey"),
            "did": evt.get("did") or c.get("repo"),
            "created_at": rec.get("createdAt"),
            "text": text,
            "langs": rec.get("langs") or [],
            "hashtags": tags,
            "mentions": mnts,
            "uri": uri,
            "raw": evt,  # útil para debug en clase
        }
        return out

    async def run():
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: orjson.dumps(v)
        )
        await producer.start()
        try:
            while True:
                try:
                    async with websockets.connect(JETSTREAM_URL, open_timeout=30) as ws:
                        print("Conectado a Jetstream:", JETSTREAM_URL)
                        async for message in ws:
                            try:
                                evt = json.loads(message)
                                # Jetstream ya viene filtrado a posts por 'wantedCollections'
                                if evt.get("kind") != "commit":
                                    continue
                                if evt.get("commit", {}).get("operation") != "create":
                                    continue
                                rec = build_record(evt)
                                if rec:
                                    # clave por DID para particionado estable
                                    key = (rec["did"] or "no-did").encode("utf-8")
                                    await producer.send_and_wait(TOPIC, rec, key=key)
                                    print("→ published:", rec["created_at"], rec["did"], rec["text"][:60])
                            except Exception as e:
                                print("Error procesando mensaje:", e)
                except Exception as e:
                    print("WS desconectado / error:", e)
                    await asyncio.sleep(3)
        finally:
            await producer.stop()

    if __name__ == "__main__":
        asyncio.run(run())

Consumidor ejemplo en python: consumer_from_kafka.py

    from kafka import KafkaConsumer
    import json

    consumer = KafkaConsumer(
        "social.posts",
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="analysis-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    for msg in consumer:
        post = msg.value
        print(post["created_at"], post["did"], (post["text"] or "")[:120])

### ejecución:

Lanzar cluster de kafka:

Terminal 1:

    cd sesion3/kafka/lab3
    docker-compose up -d
    docker ps

    Crear el topico (sino existe)

    docker exec -it lab3-kafka-1 \
      kafka-topics --create --topic social.posts \
      --bootstrap-server localhost:9092 \
      --partitions 3 --replication-factor 1

Terminal 2:

    verificar dependencias:
    pip install websockets aiokafka orjson

    Ejecutar el Producer (Bluesky → Kafka)

    python jetstream_to_kafka.py

    salida similar:

    Conectado a Jetstream: wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post
    → published: 2025-10-31T15:22:41Z did:plc:xxxxx "Happy Halloween from..."
    → published: 2025-10-31T15:22:42Z did:plc:yyyyy "New blog post about..."

Terminal 3:

    Ejecutar el Consumidor (lectura desde Kafka)

    python consumer_from_kafka.py

Terminal 4: (esto demuestra que pueden haber varios consumidores al mismo canal)

    Ejecutar el Consumidor (lectura desde Kafka)

    python consumer_from_kafka.py

### se puede leer mensajes desde el topico por linea de comando

    docker exec -it lab3-kafka-1 \
      kafka-console-consumer --bootstrap-server localhost:9092 --topic social.posts --from-beginning




