FROM apache/airflow:2.10.4

# Set the working directory
WORKDIR /opt/airflow

# Copy requirements.txt to the working directory
COPY ./requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt


user airflow