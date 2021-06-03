from kafka import KafkaConsumer
import time
import json
import pymysql

consumer = KafkaConsumer('auto_test6',
                         auto_offset_reset='earliest',
                         bootstrap_servers=['10.0.9.44:18108', '10.0.9.45:18108'], consumer_timeout_ms=5000)


def get_kafka_data():
    list=[]
    for message in consumer:
        if message is not None:
            value_str = message.value.decode()
            value_dict = eval(value_str)
            list.append(value_dict)
    return list



if __name__ == '__main__':
    for i in get_kafka_data():
        print(type(i))
