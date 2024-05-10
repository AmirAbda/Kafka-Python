from confluent_kafka import Consumer, OFFSET_BEGINNING
import time

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'test-group',
    'auto.offset.reset': 'earliest'
})

topic = 'test-topic'

def assign_and_consume(consumer, topic):
    consumer.subscribe([topic])

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
        else:
            print(f"Received message: {msg.value().decode('utf-8')}")
            # Process the message here

            # Manually commit the offset after processing the message
            consumer.commit(msg)

    consumer.close()

assign_and_consume(c, topic)
