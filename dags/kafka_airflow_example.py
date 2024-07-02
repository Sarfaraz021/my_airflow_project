from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from kafka_producer_operator import KafkaProducerOperator
from kafka_consumer_operator import KafkaConsumerOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

dag = DAG('kafka_airflow_example', default_args=default_args,
          schedule_interval='@daily')

start = DummyOperator(task_id='start', dag=dag)

produce = KafkaProducerOperator(
    task_id='produce_messages',
    topic='test',
    messages=['message1', 'message2', 'message3'],
    kafka_conn_id='kafka_default',
    dag=dag
)

consume = KafkaConsumerOperator(
    task_id='consume_messages',
    group_id='group1',
    topics=['test'],
    kafka_conn_id='kafka_default',
    dag=dag
)

end = DummyOperator(task_id='end', dag=dag)

start >> produce >> consume >> end
