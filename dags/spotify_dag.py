from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from dags.spotify_ETL import run_spotify_etl

default_argsm = {
    "owner" : "airflow",
    "depends_on_past" : False,
    "start_date" : datetime (2020, 11, 8),
    "email" : ["sztaber26@gmail.com"],
    "email_on_failure" : False,
    "email_on_retry" : False,
    "retries" : 1,
    "retry_delay" : timedelta(minutes=1)
}

dag = DAG(
    "spotify_dag",
    default_args=default_argsm,
    description="Our first DAG with ETL process!",
    schedule_interval=timedelta(days=1),
)



run_etl = PythonOperator(
    task_id = "whole_spotify_etl",
    python_callable = run_spotify_etl,
    dag=dag
)

run_etl
