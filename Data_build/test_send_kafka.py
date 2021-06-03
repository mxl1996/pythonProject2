import random
from kafka import KafkaProducer
from faker import Faker
import json
import time

producer = KafkaProducer(
                            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                            bootstrap_servers=['10.0.9.44:18108','10.0.9.45:18108']
                         )
for i in range(50000):
    fake = Faker("zh_CN")
    name=fake.name()
    age=random.randint(18,50)
    uv=random.randint(100,1000)
    num1=random.randint(10,100)
    num2=random.randint(1,1000)
    t = int(round(time.time() * 1000))
    gender=random.choice('男女')

    data={
        "name":name,
        "age":age,
        "gender":gender,
        "uv": uv,
        "num1": num1,
        "num2":num2,
        "float":1.0978,
        "id":i,
        "stringtime":t
    }
    producer.send("send1", data)
    print(data)
    time.sleep(1)

producer.close()