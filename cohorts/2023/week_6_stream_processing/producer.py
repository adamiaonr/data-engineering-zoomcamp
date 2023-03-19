import os
import json
import pandas as pd
import threading
import time

from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
from pathlib import Path

DATA_FHV=Path('data/fhv/fhv_tripdata_2019-01.csv.gz')
DATA_GREEN=Path('data/green/green_tripdata_2019-01.csv.gz')
TOPIC_GREEN="rides_green"
TOPIC_FHV="rides_fhv"

class Ride:
    def __init__(self, ride:dict) -> None:
        self.pu_location_id = int(ride['pulocationid'])
        self.do_location_id = int(ride['dolocationid'])
        self.pu_datetime = ride['pickup_datetime']
        self.do_datetime = ride['dropoff_datetime']
        self.color = ""

    def add_color(self, color:str) -> None:
        self.color = color

    def __repr__(self):
        return f'{self.__class__.__name__}: {self.__dict__}'

class JsonProducer(KafkaProducer):
    def __init__(self, props:dict):
        self.producer = KafkaProducer(**props)

    @staticmethod
    def read_records(resource_path:Path) -> list[Ride]:
        """
        Read rides .csv file, return a list of ride messages to be send
        """
        records = pd.read_csv(resource_path)
        records.columns = [c.lower().replace('lpep_','') for c in records.columns]

        return records.dropna(
            subset=[
                'pulocationid', 
                'dolocationid', 
                'pickup_datetime', 
                'dropoff_datetime']
            ) \
            .head(300) \
            .apply(lambda r : Ride(r.to_dict()), axis=1) \
            .to_list()

    def publish_rides(self, topic: str, messages: list[Ride]) -> None:
        """
        Publish rides to Kafka cluster
        """
        for ride in messages:
            ride.add_color(topic.replace('rides_',''))
            try:
                record = self.producer.send(topic=topic, key=ride.pu_location_id, value=ride)
                print('Record {} successfully produced at offset {}.{}'.format(ride.pu_location_id, topic, record.get().offset))
                time.sleep(5)
            except KafkaTimeoutError as e:
                print(e.__str__())

if __name__ == "__main__":
    config = {
        'bootstrap_servers' : os.environ.get('BOOTSTRAP_SERVER'),
        'security_protocol' : 'SASL_SSL',
        'sasl_mechanism' : 'PLAIN',
        'sasl_plain_username' : os.environ.get('CLUSTER_API_KEY'),
        'sasl_plain_password' : os.environ.get('CLUSTER_API_SECRET'),
        'acks' : 1,
        'key_serializer': lambda key: str(key).encode(),
        'value_serializer': lambda x: json.dumps(x.__dict__, default=str).encode('utf-8')
    } 
    producer = JsonProducer(props=config)

    rides = {
        TOPIC_FHV: producer.read_records(resource_path=DATA_FHV),
        TOPIC_GREEN: producer.read_records(resource_path=DATA_GREEN),
    }

    threads = []
    for topic in rides:
        thread = threading.Thread(target=producer.publish_rides, args=(topic, rides[topic],))
        threads.append(thread)
        thread.start()

    for i, thread in enumerate(threads):
        thread.join()
        print(f'thread {i} finished')
