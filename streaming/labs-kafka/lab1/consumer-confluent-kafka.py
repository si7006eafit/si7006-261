# pip install confluent_kafka
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
