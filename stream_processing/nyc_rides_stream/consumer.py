from typing import List, Dict
from ride import Ride
from settings import KafkaSettings
from confluent_kafka import Consumer
import pandas as pd


class JsonConsumer:
    def __init__(self, props: Dict, topics: List):
        self.consumer = Consumer(**props)
        self.consumer.subscribe(topics)

    def consume_from_kafka(self):
        print('Consuming from Kafka started')
        # print('Available topics to consume: ', self.consumer.subscription())

        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is not None and msg.error() is None:
                    k = msg.key().decode('utf-8')
                    v = msg.value().decode('utf-8')
                    print("key = {key:12} value = {value:12}".format(key=k, value=v))
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()
        


if __name__ == '__main__':

    settings = KafkaSettings()
    settings.config_dict["group.id"] = "python-group-1"
    settings.config_dict["auto.offset.reset"] = "earliest"

    json_consumer = JsonConsumer(props=settings.config_dict, topics=[settings.KAFKA_TOPIC])
    json_consumer.consume_from_kafka()
