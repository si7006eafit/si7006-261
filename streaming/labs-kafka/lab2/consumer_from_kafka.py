import asyncio
import json
from aiokafka import AIOKafkaConsumer

TOPIC = "social.posts"
BOOTSTRAP = "localhost:9092"

async def consume():
    # Crear consumidor asíncrono
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="analysis-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    # Conectar
    await consumer.start()
    print(f"Conectado a Kafka. Esperando mensajes en '{TOPIC}'...")
    try:
        # Loop asíncrono de consumo
        async for msg in consumer:
            post = msg.value
            created = post.get("created_at", "")
            did = post.get("did", "")
            text = (post.get("text") or "")[:120]
            print(f"{created} | {did} | {text}")
    except Exception as e:
        print("Error:", e)
    finally:
        # Cerrar conexión limpia
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume())

