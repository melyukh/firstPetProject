from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="/opt/airflow/.env")

PYTHON_DIR = "/opt/airflow/scripts/python"
SPARK_DIR  = "/opt/airflow/scripts/spark"

default_args = {
    'start_date': datetime(2025, 4, 8),
    'owner': 'airflow',
    "retries": 0,
    'depends_on_past': False 
}

dag = DAG(
    'my_dag',
    default_args = default_args,
    description = 'csv_analyze_dag',
    schedule_interval = "@daily",
    catchup = False,
    tags = ['csv', 'ETL', 'dev']
)

data_extraction = BashOperator(
    task_id = 'extract',
    bash_command='/home/airflow/.local/bin/python3 load_data.py',
    cwd = PYTHON_DIR,
    dag = dag
)

data_analysis = BashOperator(
    task_id = 'analyse',
    bash_command='/home/airflow/.local/bin/python3 data_operations.py',
    cwd = SPARK_DIR,
    dag = dag
)

data_clickhouse_query = BashOperator(
    task_id = 'clickhouse_query',
    bash_command = '/home/airflow/.local/bin/python3 clickhouse_query.py',
    cwd = PYTHON_DIR,
    dag = dag
)

data_load_to_click = BashOperator(
    task_id = 'load_to_clickhouse',
    bash_command = '/home/airflow/.local/bin/python3 clickhouse_load.py',
    cwd = PYTHON_DIR,
    dag = dag
)

data_extraction >> data_analysis >> data_load_to_click >> data_clickhouse_query # type: ignore