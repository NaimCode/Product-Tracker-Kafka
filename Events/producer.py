from kafka import KafkaProducer
import time
import json
from random import seed
from random import randint

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
topic="price";
seed(1)
for i in range(0, 1000):
    print(str(i))
    producer.send(topic,[
        {"product": "Mouse", "price": randint(0, 200)},
        {"product": "Keyboard", "price": randint(20, 300)},
        {"product": "Monitor", "price": randint(100, 600)},
        {"product": "USB 32GB", "price": randint(0, 150)},
        {"product": "HDMI", "price": randint(0, 50)},
        {"product": "PC", "price": randint(200, 600)},
    ] )
    time.sleep(20)
