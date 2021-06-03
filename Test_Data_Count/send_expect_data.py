import random
from kafka import KafkaProducer
from faker import Faker
import json
import time

producer = KafkaProducer(
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    bootstrap_servers=['10.0.9.44:18108', '10.0.9.45:18108']
)
for i in range(100):
    fake = Faker("zh_CN")
    # name = fake.name()
    age = random.randint(18, 50)
    name=str("测试"+str(i))
    result=age-i
    data={
        "id": i,
        "name":name,
        "age": 10,
        "result":result
    }

    producer.send("auto_test6", data)
    print(data)
    time.sleep(1)


producer.close()
