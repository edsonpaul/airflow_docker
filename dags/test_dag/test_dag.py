from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

# Variables used by tasks


# Default args
default_args ={
    'owner': 'edsonpaul',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False
}

# Instantiate DAG
with DAG (
    dag_id='test_dag_v1',
    default_args=default_args,
    start_date=datetime(2022, 9, 1),
    schedule_interval='0 0 * * *',
    catchup=False,
    #template_searchpath='include' #include path to look for external files
) as dag:

    task0 = DummyOperator(task_id='start')

    task1 = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql='include/create_dag_runs.sql'

    )

    task0 >> task1