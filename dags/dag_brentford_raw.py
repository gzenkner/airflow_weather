from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
import requests
from airflow.models import Variable
import os

project_id = Variable.get("GOOGLE_CLOUD_PROJECT")


dataset_id = 'weather_api'
table_id = 'weather_api_table_raw'
location = 'brentford'

full_table_id = f'{project_id}.{dataset_id}.{table_id}'
client = bigquery.Client()


def get_last_timestamp(**kwargs):
    api_key = Variable.get("OPENWEATHER_API_KEY")
    lat=51.4787 # brentford / kew gardens
    lon=-0.2956 # brentford / kew gardens
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}"
    response = requests.get(url)
    weather_data = response.json()
    
    # Push the retrieved data to XCom for later use.
    kwargs['ti'].xcom_push(key='weather_data', value=weather_data)
    

def write_to_bq_raw(full_table_id, **kwargs):
    # Pull the data from XCom that was pushed by the call_api_task.
    ti = kwargs['ti']
    weather_data = ti.xcom_pull(task_ids='call_api', key='weather_data')

    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                                        write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    job = client.load_table_from_json([{'dt':weather_data["dt"], 'location':weather_data["name"], 'string': str(weather_data)}], full_table_id, job_config=job_config)
    job.result()

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id=f"openweather_dag_{location}",
    schedule_interval="@hourly", # https://airflow.apache.org/docs/apache-airflow/1.10.1/scheduler.html
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    call_api_task = PythonOperator(
        task_id="call_api",
        python_callable=get_last_timestamp
    )

    write_to_bq_raw_task = PythonOperator(
        task_id="write_to_bq_raw",
        python_callable=write_to_bq_raw,
        op_kwargs={
            "full_table_id": full_table_id
        }
    )

    # Workflow for task direction
    call_api_task >> write_to_bq_raw_task
