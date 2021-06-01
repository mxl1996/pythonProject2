import random
from kafka import KafkaProducer
from faker import Faker
import json
import time

producer = KafkaProducer(
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    bootstrap_servers=['10.0.9.44:18108', '10.0.9.45:18108']
)
for i in range(50000):
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
    if i ==100:
        data ={

        }
        producer.send("send2", data)
        print(data)
        time.sleep(1)

    else:
        data = {"num1": uv, "str_time1": t, "array_double1": [0.12, 3.14, 4.56, 5.66],
                "name": name, "money_float": 10.0, "jb": {"money_double": 10.9, "name": name, "id": i,
                                                          "jb1": {"money_double": 10.9, "name": "name", "str_time": t},
                                                          "jb2": {"money_double": 10.9, "name": "name", "id": i},
                                                          "jb3": {
                                                              "str_time": t,
                                                              "arr_str": [name, test, provice],
                                                              "uv": uv,
                                                              "test": {"id": i, "age": age, "provice": provice}

                                                          },
                                                          "arr_int1": [i, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]},
                "arr_int": [t, t,t, t], "arr_double2": [1.0, 2.0, 3.0, 5.0], "arr_string": [name, name], "id": 10,
                "sex": gender}

        producer.send("send2", data)
        print(data)
        time.sleep(1)


producer.close()
