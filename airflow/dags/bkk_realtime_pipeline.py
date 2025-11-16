from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'bi_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'bkk_realtime_pipeline',
    default_args=default_args,
    description='Real-time BKK transport data ingestion (every 10 min)',
    schedule_interval='*/10 * * * *',  # Cada 10 minutos
    start_date=datetime(2025, 11, 15),
    catchup=False,
    tags=['bkk', 'transport', 'realtime'],
) as dag:

    extract_bkk = BashOperator(
        task_id='extract_bkk_vehicles',
        bash_command='cd /home/airflow && python python/extractors/bkk_futar_extractor.py',
    )

    transform_bkk = BashOperator(
        task_id='transform_to_dwh',
        bash_command='cd /home/airflow && python python/transformers/bkk_transformer.py',
    )

    extract_bkk >> transform_bkk
