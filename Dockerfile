# Use the official Airflow image as the base image
FROM apache/airflow:2.1.4

# Install the required Python packages
RUN pip install confluent_kafka
