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
    name = fake.name()
    age = random.randint(18, 50)
    uv = random.randint(100, 1000)
    num1 = random.randint(10, 100)
    num2 = random.randint(1, 1000)
    t = int(round(time.time() * 1000))
    gender = random.choice('男女')
    test = str("研发部" + str(i))
    provice = fake.province()
    data={
        "name": name,
        "age": 10,
        "gender": gender,
        "uv": uv,
        "num1": num1,
        "num2": num2,
        "id": i,
        "stringtime": t
    }

    producer.send("auto_test1", data)
    print(data)
    time.sleep(1)


producer.close()
