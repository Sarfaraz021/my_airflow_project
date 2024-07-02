from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from kafka_hook import KafkaHook


class KafkaConsumerOperator(BaseOperator):
    @apply_defaults
    def __init__(self, group_id, topics, kafka_conn_id='kafka_default', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.group_id = group_id
        self.topics = topics
        self.kafka_conn_id = kafka_conn_id

    def execute(self, context):
        hook = KafkaHook(kafka_conn_id=self.kafka_conn_id)
        consumer = hook.get_consumer(self.group_id, self.topics)
        messages = []
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                break
            messages.append(msg.value().decode('utf-8'))
        return messages
