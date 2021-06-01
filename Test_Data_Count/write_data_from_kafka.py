from kafka import KafkaConsumer
import time
import json




consumer = KafkaConsumer('auto_test1',
                         auto_offset_reset='earliest',
                         bootstrap_servers=['10.0.9.44:18108', '10.0.9.45:18108'])
def get_kafka_data():
    for message in consumer:
        if message is not None:
            value_str = message.value.decode()
            value_dict = eval(value_str)
            print(value_dict)
            return value_dict
if __name__ == '__main__':
    get_kafka_data()



