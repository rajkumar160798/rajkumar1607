from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import os
import sys
import requests
import json
import re
from airflow.models import TaskInstance
from google.cloud import storage
from airflow.providers.google.cloud.sensors.gcs import(
    GCSObjectExistenceSensor
)
from airflow.sensors.base import(
    BaseSensorOperator
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryGetDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryUpdateTableOperator,
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator,
    BigQueryExecuteQueryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from io_cloud.email_operator import callable_func
from io_cloud import email_operator
from airflow.operators.bash_operator import BashOperator
from google.cloud import storage
import time
from airflow.decorators import dag, task, task_group

PROJECT_ID = os.environ.get("GCP_PROJECT")
GCP_ENV = PROJECT_ID.replace("anbc-", "")
RESOURCE_SA = f'gcent-datavant-ontpd@{PROJECT_ID}.iam.gserviceaccount.com'
REGION= 'us-east4'
CUR_DIR=os.path.abspath(os.path.dirname(__file__))
to_email = 'myakalar@aetna.com'
bucket_name='cleanroom-de-jump-ent-data-dev'
gcs_json_file_path= 'match-process/input/23-09/Results/match-analytics.json'
REQUEST_ID = ''

# User defined vars
TENANT = "datavant"  # the tenant with lob, ex: xyz-hcb
OWNER = 'myakalar@aetna.com'
DAG_REPO = "datavant-member-dag"
DAG_TAGS = [f"tenant:{TENANT}", f"owner:{OWNER}"]
DAG_NAME = "match"
DATASET_ID = f"datavant_ent_{GCP_ENV}"
EMAIL_LIST  = ['myakalar@aetna.com']

FS_TABLE = "clean_room_fs_digital_2309_table"
PBM_TABLE = "clean_room_pbm_digital_2309_table"
HCB_TABLE = "clean_room_hcb_digital_2309_table"
MC_TABLE = "clean_room_mc_digital_2309_table"
SPC_TABLE = "clean_room_spc_digital_2309_table"
RTL_PHMCY_TABLE = "clean_room_rtl_phmcy_digital_2309_table"
MASTER_TABLE = "datavant_master_2309"
FS_MASTER_TABLE ="fs_digital_master_2309"
DATAVANT_FINAL_TABLE = "datavant_final_2309"

TODAY = datetime.date.today()
FIRST = TODAY.replace(day=1)
LAST_MONTH = FIRST - datetime.timedelta(days=1)
YYMM = LAST_MONTH.strftime("%y%m")
#YYMM = "2306"

FOLDER_DATE = LAST_MONTH.strftime("%Y-%m-%d")
FS_SRC_FILE = f"clean_room_fs_digital_overlap_digital_2309_out_match.txt"
PBM_SRC_FILE = f"clean_room_pbm_digital_overlap_digital_2309_out_match.txt"
HCB_SRC_FILE = f"clean_room_hcb_digital_overlap_digital_2309_out_match.txt"
MC_SRC_FILE = f"clean_room_mc_digital_overlap_digital_2309_out_match.txt"
SPC_SRC_FILE = f"clean_room_spc_digital_overlap_digital_2309_out_match.txt"
RTL_PHMCY_SRC_FILE = f"clean_room_rtl_phmcy_digital_overlap_digital_2309_out_match.txt"
SRC_FILE_PATH = "match-process/input/23-09"

DAG_ID = f"{TENANT}-{DAG_REPO}-{DAG_NAME}" # tenant-repo-filename
start_time_str=str(time.time())
BUCKET_ID = f"cleanroom-de-jump-ent-data-{GCP_ENV}"

SQL_PATH = f'{CUR_DIR.replace("py", "sql")}' + f"/creating_tables_and_inserting_data.sql"
SECOND_SQL_PATH = f'{CUR_DIR.replace("py", "sql")}' + f"/inserting_data_from_five_tables_into_master.sql"
SQL_PATH_FS_TABLE = f'{CUR_DIR.replace("py", "sql")}' + f"/creating_fs_master_table_and_inserting_data.sql"
MATCH_ANALYTICS = f'{CUR_DIR.replace("py", "sql")}' + f"/match-analaytics.sql"
MATCH_ANALYTICS_JSON = f'{CUR_DIR.replace("py", "sql")}' + f"/match-analytics.json"
MATCH_PROCESS_STEP_1 =  f'{CUR_DIR.replace("py", "sql")}' + f"/matching_master_and_fs_on_token_4_into_token_4_distinct.sql"
MATCH_PROCESS_STEP_2 = f'{CUR_DIR.replace("py", "sql")}' + f"/joining_matched_token_4.sql"
MATCH_PROCESS_STEP_3 = f'{CUR_DIR.replace("py", "sql")}' + f"/delete_matched_token_4_from_fs.sql"
MATCH_PROCESS_STEP_4 =  f'{CUR_DIR.replace("py", "sql")}' + f"/matching_master_and_fs_on_token_12_into_token_12_distinct.sql"
MATCH_PROCESS_STEP_5 = f'{CUR_DIR.replace("py", "sql")}' + f"/joining_matched_token_12.sql"
MATCH_PROCESS_STEP_6 = f'{CUR_DIR.replace("py", "sql")}' + f"/delete_matched_token_12_from_fs.sql"
MATCH_PROCESS_STEP_7 =  f'{CUR_DIR.replace("py", "sql")}' + f"/matching_master_and_fs_on_token_17_into_token_17_distinct.sql"
MATCH_PROCESS_STEP_8 = f'{CUR_DIR.replace("py", "sql")}' + f"/joining_matched_token_17.sql"
MATCH_PROCESS_STEP_9 = f'{CUR_DIR.replace("py", "sql")}' + f"/delete_matched_token_17_from_fs.sql"
MATCH_PROCESS_STEP_10 =  f'{CUR_DIR.replace("py", "sql")}' + f"/matching_master_and_fs_on_token_9_into_token_9_distinct.sql"
MATCH_PROCESS_STEP_11 = f'{CUR_DIR.replace("py", "sql")}' + f"/joining_matched_token_9.sql"
MATCH_PROCESS_STEP_12 = f'{CUR_DIR.replace("py", "sql")}' + f"/delete_matched_token_9_from_fs.sql"
MATCH_PROCESS_STEP_13 =  f'{CUR_DIR.replace("py", "sql")}' + f"/matching_master_and_fs_on_token_30_into_token_30_distinct.sql"
MATCH_PROCESS_STEP_14 = f'{CUR_DIR.replace("py", "sql")}' + f"/joining_matched_token_30.sql"
MATCH_PROCESS_STEP_15 = f'{CUR_DIR.replace("py", "sql")}' + f"/delete_matched_token_30_from_fs.sql"
MATCH_PROCESS_STEP_16 =  f'{CUR_DIR.replace("py", "sql")}' + f"/matching_master_and_fs_on_token_29_into_token_29_distinct.sql"
MATCH_PROCESS_STEP_17 = f'{CUR_DIR.replace("py", "sql")}' + f"/joining_matched_token_29.sql"
MATCH_PROCESS_STEP_18 = f'{CUR_DIR.replace("py", "sql")}' + f"/delete_matched_token_29_from_fs.sql"
MATCH_PROCESS_STEP_19 = f'{CUR_DIR.replace("py", "sql")}' + f"/generate_dv_match_id_for_records.sql"
MATCH_PROCESS_STEP_20 = f'{CUR_DIR.replace("py", "sql")}' + f"/deleting_duplicates.sql"


default_args = dict(
                    project_id = PROJECT_ID,
                    retries = 1,
                    email_on_failure = False,
                    email_on_retry = False,
                    email=to_email
                    )
email_operator.recipient_email_list=default_args["email"] # This is mandatory, list of email recepients else dag fails.

@task(task_id="print_variables")
def printvariables():
    print(f"test environment = {GCP_ENV}")
    print(f"DAG_ID = {DAG_ID}")
    print("Addy date: "+FOLDER_DATE)
    print(FS_SRC_FILE)
    print(PBM_SRC_FILE)
    print(HCB_SRC_FILE)
    print(MC_SRC_FILE)
    print(SPC_SRC_FILE)
    print(RTL_PHMCY_SRC_FILE)

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime.datetime(2023, 10 ,11),
    tags=DAG_TAGS,
    catchup=False
    ) as dag:
    creating_tables = BigQueryInsertJobOperator(
        task_id="creating_tables",
        configuration={
            "query": {
                "query": open(SQL_PATH, "r").read().format(**locals()),
                "useLegacySql": False
            }
        },
       impersonation_chain=RESOURCE_SA
    )
    creating_master_table = BigQueryInsertJobOperator(
        task_id="creating_master_table",
        configuration={
            "query": {
                "query": open(SECOND_SQL_PATH, "r").read().format(**locals()),
                "useLegacySql": False
            }
        },
        impersonation_chain=RESOURCE_SA
    )
    joining_matched_token_17_into_master = BigQueryInsertJobOperator(
        task_id="joining_matched_token_17_into_master",
        configuration={
            "query": {
                "query": open(MATCH_PROCESS_STEP_8, "r").read().format(**locals()),
                "useLegacySql": False
            }
        },
        impersonation_chain=RESOURCE_SA
    )
    delete_matched_token_17_from_fs = BigQueryInsertJobOperator(
        task_id="delete_matched_token_17_from_fs",
        configuration={
            "query": {
                "query": open(MATCH_PROCESS_STEP_9, "r").read().format(**locals()),
                "useLegacySql": False
            }
        },
        impersonation_chain=RESOURCE_SA
    )
    matching_master_and_fs_on_token_9 = BigQueryInsertJobOperator(
        task_id="matching_master_and_fs_on_token_9",
        configuration={
            "query": {
                "query": open(MATCH_PROCESS_STEP_10, "r").read().format(**locals()),
                "useLegacySql": False
            }
        },
        impersonation_chain=RESOURCE_SA
    )
    joining_matched_token_9_into_master = BigQueryInsertJobOperator(
        task_id="joining_matched_token_9_into_master",
        configuration={
            "query": {
                "query": open(MATCH_PROCESS_STEP_11, "r").read().format(**locals()),
                "useLegacySql": False
            }
        },
        impersonation_chain=RESOURCE_SA
    )
    delete_matched_token_9_from_fs = BigQueryInsertJobOperator(
        task_id="delete_matched_token_9_from_fs",
        configuration={
            "query": {
                "query": open(MATCH_PROCESS_STEP_12, "r").read().format(**locals()),
                "useLegacySql": False
            }
        },
        impersonation_chain=RESOURCE_SA
    )
    matching_master_and_fs_on_token_30 = BigQueryInsertJobOperator(
        task_id="matching_master_and_fs_on_token_30",
        configuration={
            "query": {
                "query": open(MATCH_PROCESS_STEP_13, "r").read().format(**locals()),
                "useLegacySql": False
            }
        },
        impersonation_chain=RESOURCE_SA
    )
    joining_matched_token_30_into_master = BigQueryInsertJobOperator(
        task_id="joining_matched_token_30_into_master",
        configuration={
            "query": {
                "query": open(MATCH_PROCESS_STEP_14, "r").read().format(**locals()),
                "useLegacySql": False
            }
        },
        impersonation_chain=RESOURCE_SA
    )
    delete_matched_token_30_from_fs = BigQueryInsertJobOperator(
        task_id="delete_matched_token_30_from_fs",
        configuration={
            "query": {
                "query": open(MATCH_PROCESS_STEP_15, "r").read().format(**locals()),
                "useLegacySql": False
            }
        },
        impersonation_chain=RESOURCE_SA
    )
    matching_master_and_fs_on_token_29 = BigQueryInsertJobOperator(
        task_id="matching_master_and_fs_on_token_29",
        configuration={
            "query": {
                "query": open(MATCH_PROCESS_STEP_16, "r").read().format(**locals()),
                "useLegacySql": False
            }
        },
        impersonation_chain=RESOURCE_SA
    )
    joining_matched_token_29_into_master = BigQueryInsertJobOperator(
        task_id="joining_matched_token_29_into_master",
        configuration={
            "query": {
                "query": open(MATCH_PROCESS_STEP_17, "r").read().format(**locals()),
                "useLegacySql": False
            }
        },
        impersonation_chain=RESOURCE_SA
    )
    delete_matched_token_29_from_fs = BigQueryInsertJobOperator(
        task_id="delete_matched_token_29_from_fs",
        configuration={
            "query": {
                "query": open(MATCH_PROCESS_STEP_18, "r").read().format(**locals()),
                "useLegacySql": False
            }
        },
        impersonation_chain=RESOURCE_SA
    )
    generating_dv_match_id_for_records = BigQueryInsertJobOperator(
        task_id="generating_dv_match_id_for_records",
        configuration={
            "query": {
                "query": open(MATCH_PROCESS_STEP_19, "r").read().format(**locals()),
                "useLegacySql": False
            }
        },
        impersonation_chain=RESOURCE_SA
    )
    printenvvariables = printvariables()


 def sanitize_task_id(text):
    # Replaces any non-alphanumeric characters with underscores
    sanitized = re.sub(r'[^a-zA-Z0-9._-]+', '_', text)
    # Ensures it doesn't start with digits or special characters
    sanitized = re.sub(r'^[^a-zA-Z]+', 'task_', sanitized)
    return sanitized
 
def generate_analytics_json(**context):
    bigquery_hook = BigQueryHook(
        use_legacy_sql=False,
        impersonation_chain=RESOURCE_SA
    )
    total_records = { }
    metadata = {'match_logic':'waterfall',"start_time":start_time_str}

    with open(MATCH_ANALYTICS, 'r') as sql_file:
        sql_queries = sql_file.read().split(';')
    for query in sql_queries:
        query = query.strip()
        if query:
            parts = query.lower().split(" as ")
            if len(parts) > 1:
                task_id_name_part = parts[1].split()[0]
                task_id = sanitize_task_id(task_id_name_part)
 
                if not task_id:
                    print(f"Skipping query due to invalid task_id: {task_id_name_part}")
                    continue
                result_data = bigquery_hook.get_pandas_df(sql=query)
                count_value =int(result_data.iloc [0,0])
                total_records[task_id]=count_value

    final_output = {
        'metadata': metadata,
        'total': total_records
    }
    ti: TaskInstance = context['ti']
    ti.xcom_push(key='match_analytics', value={'total': final_output})
 
# Updating the PythonOperator in your DAG definition
generate_json_task = PythonOperator(
    task_id='generate_analytics_json',
    python_callable=generate_analytics_json,
    provide_context=True,
    dag=dag,
)
 
def write_content_to_json_and_upload(bucket_name,object_name,content):
    client=storage.Client()
    bucket=client.bucket(bucket_name)
    blob=bucket.blob(object_name)
    blob.upload_from_string(json.dumps(content,indent=2),content_type='application/json')
    print(f"File {object_name} uploaded to {bucket_name}")
 
def view_results(**context):
    ti: TaskInstance = context['ti']
    results = ti.xcom_pull(task_ids='generate_analytics_json', key='match_analytics')
    write_content_to_json_and_upload(bucket_name,gcs_json_file_path,results)
    print("Results:", results)

 
view_results_task = PythonOperator(
    task_id='view_results',
    python_callable=view_results,
    provide_context=True,
    dag=dag,

)


 




# Constants (replace with your actual values)
RESOURCE_SA = 'your-service-account'
MATCH_ANALYTICS = 'your-match-analytics-file-path'
bucket_name = 'your-bucket-name'
gcs_json_file_path = 'your-gcs-json-file-path'
dag = 'your-dag-object'  # Replace with your actual DAG object

def sanitize_task_id(text):
    # Replaces any non-alphanumeric characters with underscores
    sanitized = re.sub(r'[^a-zA-Z0-9._-]+', '_', text)
    # Ensures it doesn't start with digits or special characters
    sanitized = re.sub(r'^[^a-zA-Z]+', 'task_', sanitized)
    return sanitized
 
def generate_analytics_json(**context):
    bigquery_hook = BigQueryHook(
        use_legacy_sql=False,
        impersonation_chain=RESOURCE_SA
    )
    total_records = {}
    individual_frequencies = {}
    metadata = {'match_logic':'waterfall', "start_time": context['start_time_str']}

    with open(MATCH_ANALYTICS, 'r') as sql_file:
        sql_queries = sql_file.read().split(';')
    for query in sql_queries:
        query = query.strip()
        if query:
            parts = query.lower().split(" as ")
            if len(parts) > 1:
                task_id_name_part = parts[1].split()[0]
                task_id = sanitize_task_id(task_id_name_part)

                if not task_id:
                    print(f"Skipping query due to invalid task_id: {task_id_name_part}")
                    continue
                result_data = bigquery_hook.get_pandas_df(sql=query)
                
                # Handle individual frequency and counts query
                if 'individual_frequency' in result_data.columns and 'individuals' in result_data.columns:
                    for _, row in result_data.iterrows():
                        frequency = str(row['individual_frequency'])
                        count = row['individuals']
                        individual_frequencies[frequency] = count
                else:
                    count_value = int(result_data.iloc[0, 0])
                    total_records[task_id] = count_value

    final_output = {
        'metadata': metadata,
        'total': {
            **total_records,
            'individuals_frequency_counts': individual_frequencies
        }
    }
    ti: TaskInstance = context['ti']
    ti.xcom_push(key='match_analytics', value={'total': final_output})
 
def generate_analytics_json(**context):
    bigquery_hook = BigQueryHook(
        use_legacy_sql=False,
        impersonation_chain=RESOURCE_SA
    )
    total_records = {}
    individual_frequencies = {}
    token_fill_rates = {}
    metadata = {'match_logic': 'waterfall', "start_time": context['start_time_str']}

    with open(MATCH_ANALYTICS, 'r') as sql_file:
        sql_queries = sql_file.read().split(';')
    for query in sql_queries:
        query = query.strip()
        if query:
            result_data = bigquery_hook.get_pandas_df(sql=query)

            # Check for token fill rates query
            if 'tokens' in query.lower() and 'fill rate' in query.lower():
                for _, row in result_data.iterrows():
                    token = row['Tokens']
                    fill_rate = row['Fill rate']
                    token_fill_rates[token] = fill_rate
            elif 'individual_frequency' in query:
                for _, row in result_data.iterrows():
                    frequency = str(row['individual_frequency'])
                    count = row['individuals']
                    individual_frequencies[frequency] = count
            else:
                parts = query.lower().split(" as ")
                if len(parts) > 1:
                    task_id_name_part = parts[1].split()[0]
                    task_id = sanitize_task_id(task_id_name_part)

                    if not task_id:
                        print(f"Skipping query due to invalid task_id: {task_id_name_part}")
                        continue

                    count_value = int(result_data.iloc[0, 0])
                    total_records[task_id] = count_value

    final_output = {
        'metadata': metadata,
        'total': {
            **total_records,
            'individuals_frequency_counts': individual_frequencies,
            'token_fill_rates': token_fill_rates
        }
    }
    ti: TaskInstance = context['ti']
    ti.xcom_push(key='match_analytics', value={'total': final_output})
