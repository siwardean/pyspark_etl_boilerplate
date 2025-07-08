from kafka import KafkaProducer
import json

def send_to_kafka(df, topic, brokers):
    producer = KafkaProducer(
        bootstrap_servers=brokers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    for row in df.toJSON().collect():
        producer.send(topic, row)
