from airflow.hooks.base_hook import BaseHook
from confluent_kafka import Producer, Consumer


class KafkaHook(BaseHook):
    def __init__(self, kafka_conn_id='kafka_default'):
        self.conn_id = kafka_conn_id
        self.connection = self.get_connection(kafka_conn_id)

    def get_producer(self):
        producer = Producer({
            'bootstrap.servers': self.connection.host
        })
        return producer

    def get_consumer(self, group_id, topics):
        consumer = Consumer({
            'bootstrap.servers': self.connection.host,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        })
        consumer.subscribe(topics)
        return consumer
