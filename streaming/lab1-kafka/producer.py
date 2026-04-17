import json
import random
import time
from datetime import datetime, timezone
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC = "sensor_events_raw"

producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

# Sensores simulados
SENSORES = [
    "sensor-001",
    "sensor-002",
    "sensor-003",
    "sensor-004",
    "sensor-005",
    "sensor-006",
]


def delivery_report(err, msg):
    if err is not None:
        print(f"Error al enviar mensaje: {err}")
    else:
        print(
            f"Enviado a {msg.topic()} [{msg.partition()}] "
            f"offset={msg.offset()} key={msg.key().decode('utf-8')}"
        )


def generar_evento():
    sensor_id = random.choice(SENSORES)

    # Simulación básica por tipo de sensor
    base_temp = {
        "sensor-001": 24.0,
        "sensor-002": 26.5,
        "sensor-003": 28.0,
        "sensor-004": 29.5,
        "sensor-005": 31.0,
        "sensor-006": 33.0,
    }[sensor_id]

    temperatura = round(random.normalvariate(base_temp, 1.8), 2)

    evento = {
        "sensor_id": sensor_id,
        "tipo": "temperatura",
        "valor": temperatura,
        "unidad": "C",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    return sensor_id, evento


if __name__ == "__main__":
    print("Generando eventos simulados cada 2 segundos. Ctrl+C para terminar.")
    try:
        while True:
            key, evento = generar_evento()

            producer.produce(
                TOPIC,
                key=key.encode("utf-8"),
                value=json.dumps(evento).encode("utf-8"),
                callback=delivery_report
            )
            producer.poll(0)

            print("Evento generado:", json.dumps(evento, ensure_ascii=False))
            time.sleep(2)

    except KeyboardInterrupt:
        print("\nFinalizando productor...")
    finally:
        producer.flush()

