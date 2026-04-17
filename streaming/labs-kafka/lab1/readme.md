# simular un cluster apache kafka con raft

## 1. crear una VM en AWS

    t3.medium, 30 GB DD
    ubuntu 22.04
    Docker + Docker Compose

    sudo apt update
    sudo apt install docker.io -y
    sudo apt install docker-compose -y
    sudo systemctl enable docker
    sudo systemctl start docker
    sudo usermod -a -G docker ubuntu 

-- clonar el repo de la materia en /home/ubuntu

## 2. docker compose de simulación de 3 brokers en modo KRaft

-- iniciar cluster:

-- generar CLUSTER_ID:

    docker run --rm confluentinc/cp-kafka:7.5.0 kafka-storage random-uuid

-- con el resultado, crear .env: 

    docker compose up -d
    docker compose ps

## Ver el quórum Raft:

    docker exec -it kafka-1 kafka-metadata-quorum --bootstrap-server kafka-1:9092 describe --status

## Revisar el cluster.id grabado:

    docker exec -it kafka-1 grep cluster.id /var/lib/kafka/data/meta.properties

## Crear tópicos (replicación y particiones)

### 2 topicos:

    # Topico con 6 particiones, RF=3
    docker exec -it kafka-1 kafka-topics \
    --bootstrap-server kafka-1:9092 \
    --create --topic pedidos \
    --partitions 6 --replication-factor 3

    # Topico con 4 particiones, RF=3
    docker exec -it kafka-1 kafka-topics \
    --bootstrap-server kafka-1:9092 \
    --create --topic transacciones \
    --partitions 4 --replication-factor 3

### Ver listado y detalles

    docker exec -it kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --list

    docker exec -it kafka-1 kafka-topics \
    --bootstrap-server kafka-1:9092 \
    --describe --topic pedidos

### productores:

    docker exec -it kafka-1 kafka-console-producer --bootstrap-server kafka-1:9092 --topic pedidos

### consumidores: al menos 2 terminales

    docker exec -it kafka-1 kafka-console-consumer --bootstrap-server kafka-1:9092 --topic pedidos --group grupoA

### Ver estado del grupo

    docker exec -it kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:9092 --list

    docker exec -it kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:9092 --describe --group grupoA

### Prueba de fallo (líder de partición y controller)

-- líderes actuales:

    docker exec -it kafka-1 kafka-topics \
    --bootstrap-server kafka-1:9092 \
    --describe --topic pedidos | grep Leader

-- Apaga un broker

    docker stop kafka-2

-- monitorear de nuevo

    docker exec -it kafka-1 kafka-topics \
    --bootstrap-server kafka-1:9092 \
    --describe --topic pedidos

-- reingresa de nuevo:

    docker start kafka-2

-- monitorear de nuevo

    docker exec -it kafka-1 kafka-topics \
    --bootstrap-server kafka-1:9092 \
    --describe --topic pedidos

-- Revisa el quórum Raft nuevamente

    docker exec -it kafka-1 kafka-metadata-quorum \
    --bootstrap-server kafka-1:9092 describe --status

# productor y consumidor con python 

## Producer (confluent-kafka):

    from confluent_kafka import Producer
    p = Producer({'bootstrap.servers': 'localhost:19092,localhost:29092,localhost:39092'})
    for i in range(10):
        p.produce('pedidos', key=f'user-{i%3}', value=f'orden-{i}')
    p.flush()

## Consumer group

    from confluent_kafka import Consumer
    c = Consumer({
        'bootstrap.servers': 'localhost:19092,localhost:29092,localhost:39092',
        'group.id': 'grupoA',
        'auto.offset.reset': 'earliest'
    })
    c.subscribe(['pedidos'])
    while True:
        msg = c.poll(1.0)
        if msg is None: 
            continue
        print(msg.key(), msg.value())


# Ejercicios de lab kafka: (opcional)

## Consumer Groups y particiones

- Crea un topico clientes con --partitions 3 --replication-factor 3.
- crear 4 consumidores en el mismo grupo y confirma que 1 queda idle.
- cerrar uno de los consumidores activos y verifica el rebalanceo.

## Orden por clave

- Producir mensajes a transacciones usando key user_id (con producer console con key:valor).
- Verifica que el mismo user_id va siempre a la misma partición.

## Replicación y pérdida de nodos

- Con pedidos (RF=3), apaga un nodo: lectura y escritura deben continuar.
- Apaga dos nodos: observa errores por falta de quórum (no debería aceptar escrituras con acks=all).

# bajar el cluster:

    docker compose down -v
