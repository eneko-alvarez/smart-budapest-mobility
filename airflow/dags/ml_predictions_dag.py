from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "eneko",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

common_env = {
    "DB_HOST": "bi_postgres",
    "DB_PORT": "5432",
    "DB_USER": "bi_user",
    "DB_PASSWORD": "bi_password_secure",
    "DB_NAME": "bi_budapest",
}

with DAG(
    dag_id="ml_predictions_dag",
    default_args=default_args,
    description="Generate ML predictions for next day transport demand",
    start_date=datetime(2025, 12, 3),
    schedule_interval="30 23 * * *",  # Ejecuta cada día a las 23:30 (11:30 PM)
    catchup=False,
    max_active_runs=1,
    tags=["ml", "predictions"],
) as dag:

    generate_predictions = BashOperator(
        task_id="generate_predictions",
        bash_command="python /home/airflow/python/ml/predict_transport_demand.py",
        env=common_env,
        execution_timeout=timedelta(minutes=15),
    )
