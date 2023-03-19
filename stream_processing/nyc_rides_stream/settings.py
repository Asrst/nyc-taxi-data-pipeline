import os

class KafkaSettings:
    def __init__(self):
        self.config_dict = {
            'bootstrap.servers': "pkc-41p56.asia-south1.gcp.confluent.cloud:9092",
            'security.protocol': "SASL_SSL",
            'sasl.mechanisms': "PLAIN",
            'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
            'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
        }

        self.KAFKA_TOPIC = "nyc_rides"
        self.INPUT_DATA_PATH = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz'

        # Best practice for higher availability in librdkafka clients prior to 1.7
        # session.timeout.ms = 45000

        # # Required connection configs for Confluent Cloud Schema Registry
        # schema.registry.url=https://{{ SR_ENDPOINT }}
        # basic.auth.credentials.source=USER_INFO
        # basic.auth.user.info={{ SR_API_KEY }}:{{ SR_API_SECRET }}
