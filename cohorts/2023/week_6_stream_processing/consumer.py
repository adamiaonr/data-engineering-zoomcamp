import os
import json
import pandas as pd

from kafka import KafkaConsumer

TOPIC_ALL="rides_all"

if __name__ == "__main__":
    config = {
        'bootstrap_servers' : os.environ.get('BOOTSTRAP_SERVER'),
        'security_protocol' : 'SASL_SSL',
        'sasl_mechanism' : 'PLAIN',
        'sasl_plain_username' : os.environ.get('CLUSTER_API_KEY'),
        'sasl_plain_password' : os.environ.get('CLUSTER_API_SECRET'),
        'key_deserializer': lambda key: int(key.decode('utf-8')),
        'value_deserializer': lambda m: json.loads(m.decode('utf-8')),
        'auto_offset_reset': 'latest',
        'enable_auto_commit': True,
        'group_id': 'consumer.group.id.hw6.1',
    } 

    consumer = KafkaConsumer(**config)
    consumer.subscribe([TOPIC_ALL])

    print('consuming from Kafka started')
    print('available topics to consume: ', consumer.subscription())

    location_count = pd.DataFrame(columns=['pu_location_id', 'count'])

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            message = consumer.poll(1.0)
            if message is None or message == {}:
                continue

            for _, message_value in message.items():
                message_data = pd.DataFrame([v.value for v in message_value])
                location_count = pd.concat(
                    [
                        location_count,
                        message_data
                    ]
                ) \
                    .drop_duplicates(subset=['pu_location_id'], keep='last') \
                    .sort_values(by=['count'], ascending=False) \
                    .reset_index(drop=True)

            print("---")
            print(
                location_count.sort_values(by=['count'], ascending=False) \
                    .head(5) \
                    .to_markdown(index=False)
                )

        except KeyboardInterrupt:
            break

    consumer.close()
