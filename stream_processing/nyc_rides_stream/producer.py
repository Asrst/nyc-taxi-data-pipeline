import os, sys
import json
from typing import List, Dict
from ride import Ride
from settings import KafkaSettings
from confluent_kafka import Producer
import pandas as pd


def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                            (msg.topic(), msg.partition(), msg.offset()))


class JsonProducer(Producer):
    def __init__(self, props: Dict, topic: str):
        # self.resources = './resources/rides.csv'
        self.producer = Producer(**props)
        self.topic = topic

    @staticmethod
    def read_records(resource_path: str):
        records = []
        df = pd.read_csv(resource_path)
        for idx, row in df.iterrows():
            records.append(Ride.from_dict(row.to_dict()))
        return records

    def publish_rides(self, messages: List[Ride]):
        for ride in messages:
            try:
                key = str(ride.pu_location_id).encode()
                value = json.dumps(ride.__dict__, default=str).encode('utf-8')
                self.producer.produce(topic=self.topic, key=key, value=value, 
                                      callback=delivery_callback)
                self.producer.poll(1)
            except Exception as e:
                print(e.__str__())

        # Block until the messages are sent.
        self.producer.flush()


if __name__ == '__main__':
    # Config Should match with the KafkaProducer expectation
    
    settings = KafkaSettings()
    # settings.config_dict["key_serializer"] = lambda key: str(key).encode()
    # settings.config_dict["value_serializer"] = lambda x: json.dumps(x.__dict__, default=str).encode('utf-8')

    producer = JsonProducer(props=settings.config_dict, topic=settings.KAFKA_TOPIC)
    rides = producer.read_records(resource_path=settings.INPUT_DATA_PATH)
    producer.publish_rides(messages=rides)
