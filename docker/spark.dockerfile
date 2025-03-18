# Use the official Bitnami Spark image as the base
FROM bitnami/spark:latest

USER root

RUN install_packages curl

RUN apt-get update && apt-get install -y procps

# Set environment variables for Spark
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Download and add the PostgreSQL JDBC driver
RUN curl  https://jdbc.postgresql.org/download/postgresql-42.2.23.jar -o /opt/bitnami/spark/jars/postgresql-42.2.23.jar && \
    curl https://github.com/GoogleCloudDataproc/spark-bigquery-connector/releases/download/0.36.5/spark-3.5-bigquery-0.36.5.jar  -o /opt/bitnami/spark/jars/spark-3.5-bigquery-0.36.5.jar
    
