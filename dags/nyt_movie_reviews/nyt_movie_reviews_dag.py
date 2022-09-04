from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

from datetime import datetime, timedelta
import os

# Local modules
from nyt_movie_reviews.utils import api_to_s3

# Variables used by tasks
DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
NYT_API_TOKEN = Variable.get('NYT_API_TOKEN')
NYT_API_SECRET = Variable.get('NYT_API_SECRET')

email_notification_list = Variable.get('email_notification_list')
bucket_name = 'nyt'
paths = ['critics', 'reviews']

# Default args
default_args ={
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'email': ['info@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

# Instantiate DAG
with DAG (
    DAG_ID,
    default_args=default_args,
    start_date=datetime(2022, 9, 1),
    schedule_interval='0 0 * * *',
    tags=["nyt"],
    catchup=False,
    #template_searchpath='{DAG_ID}/include/' #include path to look for external files
) as dag:

    task0 = DummyOperator(task_id='start')

    # Define Task Group with Python Operator API extracts. Loop through paths provided
    with TaskGroup('api_paths_extract') as api_paths_extract:
        for path in paths:
            extracts = PythonOperator(
                task_id="{0}_api_to_s3".format(path),
                dag=dag,
                python_callable=api_to_s3,
                op_kwargs={
                    "path": path,
                    "access_key": NYT_API_TOKEN,
                    "secret_key": NYT_API_SECRET,
                    "bucket_name": bucket_name,
                }
            )

    # task2 = PythonOperator(
    #     task_id="critics_api_to_s3",
    #     dag=dag,
    #     python_callable=api_to_s3,
    #     op_kwargs={
    #         "path": "critics",
    #         "access_key": NYT_API_TOKEN,
    #         "secret_key": NYT_API_SECRET,
    #         "bucket_name": bucket_name,
    #         }
    # )

    # Define task to send email
    send_email = EmailOperator(
        task_id='send_email',
        to=email_notification_list,
        subject='NYT Movie Reviews DAG',
        html_content='<p>The NYT Movie Reviews DAG completed successfully. <p>'
    )

    task0 >> api_paths_extract >> send_email