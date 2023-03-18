import os
import json
import pandas as pd

from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
from pathlib import Path

DATA_FHV=Path('data/fhv/fhv_tripdata_2019-01.csv.gz')
DATA_GREEN=Path('data/green/green_tripdata_2019-01.csv.gz')

class Ride:
    def __init__(self, ride:dict) -> None:
        # make keys uniform
        ride = { k.lower().replace('lpep_','') : v for k,v in ride.items() }
        # build object
        self.pu_location_id = ride['pulocationid']
        self.do_location_id = ride['dolocationid']
        self.pu_datetime = ride['pickup_datetime']
        self.do_datetime = ride['dropoff_datetime']

class JsonProducer(KafkaProducer):
    def __init__(self, props:dict):
        self.producer = KafkaProducer(**props)

    @staticmethod
    def read_records(resource_path:Path) -> list[Ride]:
        """
        Read rides .csv file, return a list of ride messages to be send
        """
        return pd.read_csv(resource_path)\
            .head(20)\
            .apply(lambda r : Ride(r.to_dict()), axis=1)\
            .to_list()

    def publish_rides(self, topic: str, messages: list[Ride]) -> None:
        """
        Publish rides to Kafka cluster
        """
        for ride in messages:
            try:
                record = self.producer.send(topic=topic, key=ride.pu_location_id, value=ride)
                print('Record {} successfully produced at offset {}'.format(ride.pu_location_id, record.get().offset))
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
        'rides_fhv': producer.read_records(resource_path=DATA_FHV),
        'rides_green': producer.read_records(resource_path=DATA_GREEN),
    }

    # print(rides['rides_fhv'])
    print(rides['rides_green'])

    for topic in rides:
        producer.publish_rides(topic=topic, messages=rides[topic])
