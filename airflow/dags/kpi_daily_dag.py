from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "eneko",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

common_env = {
    "DB_HOST": "postgres",
    "DB_PORT": "5432",
    "DB_USER": "bi_user",
    "DB_PASSWORD": "bi_password_secure",
    "DB_NAME": "bi_budapest",
}

with DAG(
    dag_id="kpi_daily_dag",
    default_args=default_args,
    description="Calculate and load daily KPIs from fact_weather_conditions",
    start_date=datetime(2025, 11, 10),
    schedule_interval="0 2 * * *",  # Ejecuta cada día a las 2 AM UTC
    catchup=False,
    max_active_runs=1,
    tags=["kpi", "daily"],
) as dag:

    load_kpi_daily = BashOperator(
        task_id="load_kpi_daily",
        bash_command="python /home/airflow/python/transformers/kpi_daily_loader.py",
        env=common_env
    )
