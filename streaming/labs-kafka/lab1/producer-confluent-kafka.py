# pip install confluent_kafka
from confluent_kafka import Producer
p = Producer({'bootstrap.servers': 'localhost:19092,localhost:29092,localhost:39092'})
for i in range(10):
    p.produce('pedidos', key=f'user-{i%3}', value=f'orden-{i}')
p.flush()
