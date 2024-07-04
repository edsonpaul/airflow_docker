"""
Pipeline for Studybase 
"""

import os
import sys  # pull in sys for changing the path
from pathlib import Path
import logging
import getopt
import json
from datetime import datetime, timedelta
from dataengineering.gcs_to_bq import gcs_to_bq
from dataengineering.bq_generator import bq_generator
from dataengineering.bq_copy_bq import bq_copy_bq
from dataengineering.bq_to_bq import bq_to_bq
from dataengineering.gcs_archive import gcs_archive
from dataengineering.data_catalog_2 import data_catalog
from studybase.studybase_lib import extract_postgre_tables

# ======================================================================================================================
# Defining pipeline variables and DAG
# ======================================================================================================================

cwd = os.path.join(os.path.dirname(__file__),'studybase_cfg.json')
f_cfg = open(cwd,"r")
cfg = json.load(f_cfg)

date_pattern = datetime.now().strftime("%Y%m%d")

try:
    import airflow
    env = 'airflow'
except ImportError as error:
    env = 'local'
    gcp_key_path = "D:\\Users\\esemorio\\Documents\\dev-di-00.json"
    logging.info(f"cannot import airflow lib, will attempt to run as local: {error}")

if env == "airflow":
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from airflow.models import Variable
    from airflow.utils.dates import days_ago
    from uhgrd.airflow import EmailOperator

    di_settings_json = json.loads(Variable.get("di_settings_json").replace("'", '"'))
    studybase_settings_json = json.loads(Variable.get("studybase_settings_json").replace("'", '"'))

    gcp_key_path = Variable.get('di_credentials_path')
    gcp_project = di_settings_json["ds_00_project"]
    svc_password = studybase_settings_json["svc_password_secret"]
    ssl_root_cert = di_settings_json["secrets_folder"] + "/rds-combined-ca-bundle.pem"

    data_dict_path = os.path.join(os.path.dirname(__file__),"studybase_data_dictionary.csv")
    mdm_src_script  = os.path.join(os.path.dirname(__file__), 'mdm_script/')

    # get var values based on env
    if Variable.get("is_prd", default_var=False):
        gcs_bucket = 'bkt_prd_studybase'
        delete_aws_source_file = True
    else:
        gcs_bucket = 'bkt_dev_studybase'
        delete_aws_source_file = False

    # get default emails
    notification_emails = json.loads(Variable.get("notification_emails").replace("'", '"'))

    default_args = { "owner": "airflow",
                "email": [cfg['email_list'], notification_emails['dag_failure']],
                "email_on_failure": Variable.get("is_prd", default_var=False),
                "email_on_retry": False,
                "retries": 2,
                "retry_delay": timedelta(minutes=30),
                "execution_timeout": timedelta(hours=6),
                }
    DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

    dag = DAG( DAG_ID,
            schedule_interval="0 13 * * *",
            default_args=default_args,
            start_date=datetime(2021,10,20),
            tags=['studybase', 'level2', 'tdt'],
            max_active_runs = 1,
            concurrency=30,
            catchup=False,
            )

# ======================================================================================================================
# Defining Tasks
# ======================================================================================================================

tablelist = ["coachnotification", "cohort", "cohortfirebaseapp", "cohortsetting", "datapoint", "datapointtype", "feature", "firebaseapp", "firebaseappbuild",
            "item", "itemlocation", "itempasswordviewlog", "memberassignment", "memberprogram", "participant", "participantsession", "participantstatus",
            "participantstudygroup", "participanttask", "program", "refassignmenttype", "reffitbitconfig", "refitemevent", "refitemtype", "reflocation", "refnotification",
            "refparticipanttype", "refstatus", "refuamcredential", "sensorstarttimelog", "setting", "snoozenotification", "studygroup", "tablelog", "task", "taskdependency",
            "treatment", "uamprovisionqueue", "unifiedlog", "userdatapoint", "usertype", "vw_active_participants_for_dexcom_egv"
            ]

# send email
if env == 'airflow':
    base_message = f"{DAG_ID} successful refresh at {str(datetime.utcnow())}"
    msg = "studybase"
    custom_msg = f"All tables refreshed: {msg}."
    email_content = base_message + ". " + custom_msg

    email_success = EmailOperator(
          to=[notification_emails['dag_success']],
          subject=base_message,
          html_content=email_content,
          email_on_failure=False,
          task_id="studybase_send_an_email")

# update mdm src table
mdm_src_args = {'gcp_key_path': gcp_key_path,
                'project_id': gcp_project,
                'file_path': mdm_src_script
                }

if env == 'airflow':
    mdm_src_task = PythonOperator(
        task_id="mdm_src_creation",
        op_kwargs=mdm_src_args,
        python_callable=bq_to_bq,
        dag=dag,
        provide_context=False,
    )
else:
    bq_to_bq(**mdm_src_args)

# archive mdm src table
mdm_src_archive_args = {'gcp_key_path': gcp_key_path,
                            'source_project_id': gcp_project,
                            'destination_project_id': gcp_project,
                            'source_dataset': cfg['ods_dataset'],
                            'destination_dataset': cfg['ods_arc_dataset'],
                            'destination_table_suffix': date_pattern,
                            'destination_table_expiration': '1_year',
                            'tables': 'studybase_participants_demographics'
                            }

if env == 'airflow':
    mdm_src_archive_op = PythonOperator(task_id="mdm_src_archive", dag=dag,
                            python_callable=bq_copy_bq, op_kwargs=mdm_src_archive_args, provide_context=False)
else:
    bq_copy_bq(**mdm_src_archive_args)
    print("ods archive finished")

# data catalog
dc_args = {'gcp_key_path': gcp_key_path,
            'dataset_id': cfg['final_dataset'],
            'data_dict_path': data_dict_path,
            'encoding': "ISO-8859-1",
            'tables': None
            }
if env == 'airflow':    
    data_catalog_op = PythonOperator(
        task_id="data_catalog",
        dag=dag,
        python_callable=data_catalog,
        op_kwargs=dc_args,
        provide_context=False
    )
else:
    result = data_catalog(**dc_args)

# pull data from postgre tables and upload to GCS bucket
src_to_gcs_args = {'svc_password': svc_password, 'ssl_root_cert': ssl_root_cert,
                    'gcp_key_path': gcp_key_path, 'gcs_bucket': gcs_bucket, 'tablelist': tablelist}
if env == 'airflow':
    src_to_gcs_op = PythonOperator(task_id="src_to_gcs", dag=dag, python_callable=extract_postgre_tables, op_kwargs=src_to_gcs_args, provide_context=False) 

# ======================================================================================================================
# Define parallel run of DI tasks per table
# ======================================================================================================================

for table in tablelist:
    # load CSV files from GCS bucket to ETL
    gcs_to_bq_args = { 'gcp_key_path': gcp_key_path,
                        'project_id': gcp_project,
                        'source': gcs_bucket,
                        'destination_dataset': cfg['etl_dataset'],
                        'mode': cfg['tables'][table]['sourceformat'],
                        'skip_leading_rows': 1,
                        'force_string': True,
                        'field_delimiter': ",",
                        'files': {cfg['tables'][table]['filename']: table}
                        }
   
    if env == 'airflow':
        gcs_to_bq_op = PythonOperator(task_id="gcs_to_bq_"+table, dag=dag, python_callable=gcs_to_bq, op_kwargs=gcs_to_bq_args, provide_context=False)
    else:
        print(gcs_to_bq_args)
        print ("*****Running GC to BQ*******\n\n")
        result = gcs_to_bq(**gcs_to_bq_args)
        print ("*****Finished GC to BQ*******\n\n")

    # process ETL tables to ODS
    etl_to_ods_args1 = {'source_project_id': gcp_project,
                        'destination_project_id': gcp_project,
                        'source_dataset':cfg['etl_dataset'],
                        'destination_dataset':cfg['ods_dataset'],
                        'gcp_key_path':gcp_key_path,
                        'tables':table,
                        'keys':{table:{"pkey":cfg['tables'][table]['pkey'],"qkey":cfg['tables'][table]['qkey']}},
                       "convert_string_value_lower": cfg['tables'][table]['convert_string_value_lower']
                       }

    etl_to_ods_args2 = {'operation':cfg['tables'][table]['ods_operation'],
                        'execute_immediately':cfg['tables'][table]['ods_execute_immediately'],
                        'indel_source_key':cfg['tables'][table]['pkey'],
                        'indel_param_date':cfg['tables'][table]['indel_param_date']
                        }


    if env == 'airflow':
        def _etl_to_ods(etl_to_ods_args1, etl_to_ods_args2):
            bq_generator(**etl_to_ods_args1).etl_to_ods(**etl_to_ods_args2)
        etl_to_ods_op = PythonOperator(
            task_id="etl_to_ods_"+table,
            dag=dag, 
            python_callable=_etl_to_ods, 
            op_kwargs={
                "etl_to_ods_args1":etl_to_ods_args1,
                "etl_to_ods_args2":etl_to_ods_args2
            }, 
            provide_context=False
        )
    else:
        print ("*****Running ETL to ODS*******\n\n")
        bq_generator(**etl_to_ods_args1).etl_to_ods(**etl_to_ods_args2)
        print ("*****Finished ETL to ODS*******\n\n")

    # process ODS tables to STG
    ods_stg_args1 = {'source_project_id': gcp_project,
                     'destination_project_id': gcp_project,
                     'source_dataset': cfg['ods_dataset'],
                     'destination_dataset': cfg['stg_dataset'],
                     'gcp_key_path': gcp_key_path,
                     'tables': table
                     }
    ods_stg_args2 = {'mdm_crosswalk_name': cfg['mdm_crosswalk_name'],
                     'inner_or_left_join': cfg['tables'][table]['mdm_crosswalk_join_type'],
                     'has_custom_source_key': False,
                     'execute_immediately': cfg['tables'][table]['stg_execute_immediately']
                     }

    if env == 'airflow':
        def _ods_to_stg(ods_stg_args1, ods_stg_args2):
            bq_generator(**ods_stg_args1).ods_to_stg(**ods_stg_args2)

        ods_to_stg_op = PythonOperator(
            task_id="ods_to_stg_" + table,
            dag=dag,
            python_callable=_ods_to_stg,
            op_kwargs={
                "ods_stg_args1": ods_stg_args1,
                "ods_stg_args2": ods_stg_args2
            },
            provide_context=False
        )

    else:
        print(ods_stg_args2)
        print(f"*****Running {table} ODS to STG*******\n\n")
        bq_generator(**ods_stg_args1).ods_to_stg(**ods_stg_args2)
        print("*****Finished ODS to STG*******\n\n")

    # copy tables from STG to FINAL
    stg_to_final_args = {'gcp_key_path' : gcp_key_path,
                        'source_project_id': gcp_project,
                        'destination_project_id': gcp_project,
                        'source_dataset':cfg['stg_dataset'],
                        'destination_dataset':cfg['final_dataset'],
                        'source_table_suffix':'',
                        'destination_table_suffix':'',
                        'destination_table_expiration':'',
                        'tables':table
                        }

    if env == 'airflow':
      stg_to_final_op = PythonOperator(task_id="stg_to_final_"+table, dag=dag, python_callable=bq_copy_bq, op_kwargs=stg_to_final_args, provide_context=False)
    else:
      print("*****Running STG to Final*******\n\n")   
      bq_copy_bq(**stg_to_final_args)
      print("*****Finished STG to Final*******\n\n")

    # archive ETL tables to ETL_ARC
    etl_to_archive_args = {'gcp_key_path': gcp_key_path,
                          'source_project_id': gcp_project,
                          'destination_project_id': gcp_project,
                          'source_dataset': cfg['etl_dataset'],
                          'destination_dataset': cfg['etl_arc_dataset'],
                          'source_table_suffix': '',
                          'destination_table_suffix': date_pattern,
                          'destination_table_expiration': '1_year',
                          'tables': table
                          }

    if env == 'airflow':
        etl_to_archive_op = PythonOperator(task_id="etl_to_archive_" + table, dag=dag, python_callable=bq_copy_bq,
                                          op_kwargs=etl_to_archive_args, provide_context=False)
    else:
        bq_copy_bq(**etl_to_archive_args)
        print("etl archive finished")

    # archive ODS tables to ODS_ARC
    ods_to_archive_args = {'gcp_key_path': gcp_key_path,
                          'source_project_id': gcp_project,
                          'destination_project_id': gcp_project,
                          'source_dataset': cfg['ods_dataset'],
                          'destination_dataset': cfg['ods_arc_dataset'],
                          'source_table_suffix': '',
                          'destination_table_suffix': date_pattern,
                          'destination_table_expiration': '1_year',
                          'tables': table
                          }

    if env == 'airflow':
        ods_to_archive_op = PythonOperator(task_id="ods_to_archive_" + table, dag=dag, python_callable=bq_copy_bq,
                                          op_kwargs=ods_to_archive_args, provide_context=False)
    else:
        bq_copy_bq(**ods_to_archive_args)
        print("ods archive finished")

    # archive FINAL tables to FINAL_ARC
    final_to_archive_args = {'gcp_key_path': gcp_key_path,
                            'source_project_id': gcp_project,
                            'destination_project_id': gcp_project,
                            'source_dataset': cfg['final_dataset'],
                            'destination_dataset': cfg['final_arc_dataset'],
                            'source_table_suffix': '',
                            'destination_table_suffix': date_pattern,
                            'destination_table_expiration': '1_year',
                            'tables': table
                            }

    if env == 'airflow':
        final_to_archive_op = PythonOperator(task_id="final_to_archive_" + table, dag=dag, python_callable=bq_copy_bq,
                                            op_kwargs=final_to_archive_args, provide_context=False)
    else:
        bq_copy_bq(**final_to_archive_args)

    # archive CSV files in GCS bucket
    gcs_archive_args = {'gcp_key_path': gcp_key_path,
                        'project_id': gcp_project,
                        'source': gcs_bucket,
                        'files': cfg['tables'][table]['filename'],
                        'archive_pattern': date_pattern
                        }
    if env == 'airflow':
        gcs_archive_op = PythonOperator(task_id="gcs_archive_" + table, dag=dag, python_callable=gcs_archive,
                                        op_kwargs=gcs_archive_args, provide_context=False)
    else:
        gcs_archive(**gcs_archive_args)

    src_to_gcs_op >> gcs_to_bq_op >> etl_to_archive_op >> etl_to_ods_op >> mdm_src_task
    mdm_src_task >> mdm_src_archive_op >> ods_to_archive_op >> ods_to_stg_op >> stg_to_final_op
    stg_to_final_op >> final_to_archive_op >> data_catalog_op >> gcs_archive_op >> email_success
