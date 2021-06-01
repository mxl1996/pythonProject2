import random
from kafka import KafkaProducer
from faker import Faker
import json
import time

producer = KafkaProducer(
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    bootstrap_servers=['10.0.9.44:18108', '10.0.9.45:18108']
)
for i in range(5000):
    fake = Faker("zh_CN")
    name = fake.name()
    money_double = random.uniform(401, 4)
    money_long = random.randint(100222, 1000000)
    t = int(round(time.time() * 1000))

    data = {
  "id": "0001238123899122",
  "name": "asdlkjasjkdla998y1122",
  "date": "1990-10-14",

  "obj": {
    "time1": "12:12:43Z",
    "str": "sfasfafs",
    "lg": 2324342345
 },
  "arr": [
    {
      "f1": "f1str11",
      "f2": 134
 },
    {
      "f1": "f1str22",
      "f2": 555
 }
  ],
  "time": "12:12:43Z",
  "timestamp": "1990-10-14T12:12:43Z",
  "map": {
    "flink": 1236
 },
  "mapinmap": {
    "inner_map": {
      "key": 234
 }
  }
}
    producer.send("source1", data)
    print(data)
    time.sleep(1)

producer.close()
