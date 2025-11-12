from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "eneko",
    "depends_on_past": False,
    "retries": 1,                       # menos reintentos para no esperar tanto
    "retry_delay": timedelta(minutes=2)
}

# Variables de entorno que heredarán TODAS las tareas
common_env = {
    "DB_HOST": "postgres",
    "DB_PORT": "5432",
    "DB_USER": "bi_user",
    "DB_PASSWORD": "bi_password_secure",
    "DB_NAME": "bi_budapest",
}

with DAG(
    dag_id="weather_pipeline_dag",
    default_args=default_args,
    description="Extract Open-Meteo -> staging; transform to raw; load dim_time; load fact_weather",
    start_date=datetime(2025, 11, 10),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["weather", "etl"],
) as dag:

    extract_to_staging = BashOperator(
        task_id="extract_to_staging",
        bash_command="python /home/airflow/python/transformers/weather_raw_loader.py",
        env=common_env
    )

    transform_to_raw = BashOperator(
        task_id="transform_to_raw",
        bash_command="python /home/airflow/python/transformers/weather_to_raw.py",
        env=common_env
    )

    ensure_dim_time = BashOperator(
        task_id="ensure_dim_time",
        bash_command="python /home/airflow/python/transformers/dim_time_loader.py",
        env=common_env
    )

    load_fact_weather = BashOperator(
        task_id="load_fact_weather",
        bash_command="python /home/airflow/python/transformers/weather_to_fact.py",
        env=common_env
    )

    extract_to_staging >> transform_to_raw >> ensure_dim_time >> load_fact_weather
