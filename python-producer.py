from confluent_kafka import Producer

import time 

p = Producer({'bootstrap.servers': 'localhost:9092'})

topic = 'test-topic'

for i in range(100):
    p.produce(topic, f'Message {i}')
    p.flush()
    time.sleep(1)
