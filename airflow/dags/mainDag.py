from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
# from extract import extract_data


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='solar_energy_crops_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

# extract_task = PythonOperator(
#     task_id='extract_data',
#     python_callable=extract_data,
#     dag=dag
# )

start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Jobs started"),
    dag=dag
)

first_task= SparkSubmitOperator(
    task_id="first_task",
    application="jobs/Script1.py",  # Path to your Spark script
    conn_id="spark_mm",  # Spark connection ID
    dag=dag
)

# Define the Spark job task
# submit_spark_job = SparkSubmitOperator(
#     task_id="submit_spark_job",
#     application="jobs/transform.py",  # Path to your Spark script
#     conn_id="spark_mm",  # Spark connection ID
#     dag=dag
# )

# transform_task = PythonOperator(
#     task_id='transform_data',
#     python_callable=transform_data,
#     dag=dag
# )

# load_task = PythonOperator(
#     task_id='load_data',
#     python_callable=load_data,
#     dag=dag
# )

# extract_task 

start >> first_task
