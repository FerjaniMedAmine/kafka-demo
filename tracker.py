import json

from confluent_kafka import Consumer

consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    "group.id": "order-tracker",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(consumer_config)
consumer.subscribe(["orders"])
print("consumer is running and subscribed to orders topic")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(msg.error())
        value=msg.value().decode("utf-8")
        order= json.loads(value)
        print(order)
except KeyboardInterrupt:
    print("closing consumer")
finally:
    consumer.close()