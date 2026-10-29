import json
import uuid

from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err,msg):
    if err:
        print(err)
    else:
        print(f"delivered {msg.value().decode('utf-8')}")


order= {
    "order_id" :str(uuid.uuid4()),
    "user" :'amine',
    "item" :'pizza',
    "quantity" :1,
}

value=json.dumps(order).encode("utf-8")

producer.produce(topic="orders",
                 value=value,
                 callback=delivery_report)

producer.flush()
