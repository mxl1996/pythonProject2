import random
from kafka import KafkaProducer
from faker import Faker
import json
import time

producer = KafkaProducer(
                            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                            bootstrap_servers=['10.0.9.44:18108','10.0.9.45:18108']
                         )
for i in range(5000):
    fake = Faker("zh_CN")
    name=fake.name()
    money_double=random.uniform(401,699)
    money_long=random.randint(100222,1000000)
    t = int(round(time.time() * 1000))
    gender = random.choice('男女')

    data={
  "id": i,
  "name": name,
  "gender": gender,
  "obj": {
    "num1": money_long,
    "str_time": t,
    "num2": money_double
 },
  "time": t

  }
    producer.send("send1", data)
    print(data)
    time.sleep(1)

producer.close()