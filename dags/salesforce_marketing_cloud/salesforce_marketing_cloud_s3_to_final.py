"""
Pipeline for Salesforce Marketing Cloud
"""

import datetime
from datetime import timedelta, datetime
import os, sys, json
from google.oauth2 import service_account
from uhgrd.airflow import Variable
from uhgrd.airflow import DAG
from uhgrd.airflow import days_ago
from uhgrd.airflow import PythonOperator

# ======================================================================================================================
# Defining pipeline variables
# ======================================================================================================================

# local modules
from salesforce_marketing_cloud.salesforce_marketing_cloud_lib import sfmc_s3_to_gcs, sfmc_gcs_to_bq, sfmc_gcs_archive, sfmc_stg_to_final, archive_tables
sys.path.insert(1, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dataengineering.bq_to_bq import bq_to_bq
from dataengineering.send_email import send_email

# Airflow global variables
di_settings_json = json.loads(Variable.get("di_settings_json").replace("'", '"'))
sfmc_settings_json = json.loads(Variable.get("sfmc_settings_json").replace("'", '"'))
sendgrid_api_key = Variable.get("sendgrid_api_key")
aws_access_key_id = Variable.get("cdb_aws_access_key_id")
aws_secret_access_key = Variable.get("cdb_aws_secret_access_key")

aws_source_bucket_name = 'aws-bkt-prd-sfmc'
gcp_project = di_settings_json["ds_00_project"]
arc_project = di_settings_json["ds_00_project"]
gcp_key_path = di_settings_json["credentials_path"]

# get var values based on env
if Variable.get("is_prd", default_var=False):
    gcs_bucket = 'bkt_prd_salesforce_marketing_cloud'
    delete_aws_source_file = True
else:
    gcs_bucket = 'bkt_dev_salesforce_marketing_cloud'
    delete_aws_source_file = False

data_source = 'salesforce_marketing_cloud'
etl_dataset = f'{data_source}_etl'
ods_dataset = f'{data_source}_ods'
stg_dataset = f'{data_source}_stg'
final_dataset = f'{data_source}_final'
etl_arc_dataset = f'{data_source}_etl_arc'
ods_arc_dataset = f'{data_source}_ods_arc'
final_arc_dataset = f'{data_source}_final_arc'

ods_script = f'{di_settings_json["root_folder"]}/salesforce_marketing_cloud/ods'
stg_script = f'{di_settings_json["root_folder"]}/salesforce_marketing_cloud/stg'

# Default to personal credentials (set using Google SDK in command line)
try:
    credentials = service_account.Credentials.from_service_account_file(di_settings_json["credentials_path"])
except KeyError:
    credentials = None

notification_emails = json.loads(Variable.get("notification_emails").replace("'", '"'))

# GCP logging
logger = ""  # gcp_logger(di_settings_json, pipeline_name, credentials)
# ======================================================================================================================
# Defining DAG
# ======================================================================================================================
# Don't change
DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")


# Pipeline parameters
pipeline_name = "salesforce_marketing_clouds3to_final"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": Variable.get("is_prd", default_var=False),
    "email": ["edson.semorio@optum.com", "justin.mancao@optum.com", notification_emails['dag_failure']],
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

dag = DAG(DAG_ID,
          default_args=default_args,
          schedule_interval="0 14 * * *",
          tags=["salesforce", "level2", "marketing cloud"],
          catchup=False
          )

# Convert the docstring into pretty formatting for UI
if hasattr(dag, "doc_md"):
    dag.doc_md = __doc__

# ======================================================================================================================
# Defining Tasks
# ======================================================================================================================

# Upload zip files to GCS and unzip CSV files to their corresponding folder
s3_to_gcs_task = PythonOperator(
    task_id=f"s3_to_gcs",
    op_kwargs={
        "aws_source_bucket_name": aws_source_bucket_name,
        "aws_access_key_id": aws_access_key_id,
        "aws_secret_access_key": aws_secret_access_key,
        "gcs_bucket": gcs_bucket,
        "gcp_project": gcp_project,
        "gcp_key_path": gcp_key_path,
        "delete_aws_source_file": delete_aws_source_file,
    },
    python_callable=sfmc_s3_to_gcs,
    dag=dag,
    provide_context=False,
)

# Archive ETL tables from previous run
archive_etl_task = PythonOperator(
    task_id=f"archive_etl",
    op_kwargs={
        "project_id": gcp_project,
        "dataset": etl_dataset,
        "arc_project": arc_project,
        "arc_dataset": etl_arc_dataset,
        "gcp_key_path": gcp_key_path,
    },
    python_callable=archive_tables,
    dag=dag,
    provide_context=False,
)

# Archive ODS tables from previous run
archive_ods_task = PythonOperator(
    task_id=f"archive_ods",
    op_kwargs={
        "project_id": gcp_project,
        "dataset": ods_dataset,
        "arc_project": arc_project,
        "arc_dataset": ods_arc_dataset,
        "gcp_key_path": gcp_key_path,
    },
    python_callable=archive_tables,
    dag=dag,
    provide_context=False,
)

# Archive FINAL tables from previous run
archive_final_task = PythonOperator(
    task_id=f"archive_final",
    op_kwargs={
        "project_id": gcp_project,
        "dataset": final_dataset,
        "arc_project": arc_project,
        "arc_dataset": final_arc_dataset,
        "gcp_key_path": gcp_key_path,
    },
    python_callable=archive_tables,
    dag=dag,
    provide_context=False,
)

# Load files from GCS bucket to ETL tables
gcs_to_etl_task = PythonOperator(
    task_id=f"gcs_to_etl",
    op_kwargs={
        "destination_project_id": gcp_project,
        "destination_bucket": gcs_bucket,
        "dataset": etl_dataset,
        "gcp_key_path": gcp_key_path,
    },
    python_callable=sfmc_gcs_to_bq,
    dag=dag,
    provide_context=False,
)

# Run ODS SQL scripts
etl_to_ods_task = PythonOperator(
    task_id=f"etl_to_ods",
    op_kwargs={
        "project_id": gcp_project,
        "file_path": ods_script,
        "gcp_key_path": gcp_key_path,
    },
    python_callable=bq_to_bq,
    dag=dag,
    provide_context=False,
)

# Run STG SQL scripts
ods_to_stg_task = PythonOperator(
    task_id=f"ods_to_stg",
    op_kwargs={
        "project_id": gcp_project,
        "file_path": stg_script,
        "gcp_key_path": gcp_key_path,
    },
    python_callable=bq_to_bq,
    dag=dag,
    provide_context=False,
)

# Copy tables from STG to FINAL
stg_to_final_task = PythonOperator(
    task_id=f"stg_to_final",
    op_kwargs={
        "project_id": gcp_project,
        "dataset": stg_dataset,
        "destination_project": arc_project,
        "destination_dataset": final_dataset,
        "gcp_key_path": gcp_key_path,
    },
    python_callable=sfmc_stg_to_final,
    dag=dag,
    provide_context=False,
)

# Archive inbound files
archive_inbound_files_task = PythonOperator(
    task_id=f"archive_inbound_files",
    op_kwargs={
        "destination_project_id": gcp_project,
        "gcp_key_path": gcp_key_path,
        "destination_bucket": gcs_bucket,
    },
    python_callable=sfmc_gcs_archive,
    dag=dag,
    provide_context=False,
)

subject = f"{data_source} successful run at {str(datetime.utcnow())}"
html_content = f"The {data_source} tables have been successfully refreshed."
success_notify_list = notification_emails["dag_success"]

# Send successful job run notification to teams channel
send_success_email = PythonOperator(
    task_id=f"send_success_email",
    op_kwargs={
        "to_emails": success_notify_list,
        "subject": subject,
        "html_content": html_content,
        "sendgrid_api_key": sendgrid_api_key,
    },
    python_callable=send_email,
    dag=dag,
    provide_context=False,
)

s3_to_gcs_task >> archive_etl_task >> archive_ods_task >> archive_final_task
archive_final_task >> gcs_to_etl_task
gcs_to_etl_task >> etl_to_ods_task >> ods_to_stg_task >> stg_to_final_task
stg_to_final_task >> archive_inbound_files_task >> send_success_email