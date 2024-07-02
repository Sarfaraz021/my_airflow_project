from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from kafka_hook import KafkaHook


class KafkaProducerOperator(BaseOperator):
    @apply_defaults
    def __init__(self, topic, messages, kafka_conn_id='kafka_default', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.topic = topic
        self.messages = messages
        self.kafka_conn_id = kafka_conn_id

    def execute(self, context):
        hook = KafkaHook(kafka_conn_id=self.kafka_conn_id)
        producer = hook.get_producer()
        for message in self.messages:
            producer.produce(self.topic, message)
        producer.flush()
