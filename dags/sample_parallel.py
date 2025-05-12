from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import pytz

def get_current_ist_timestamp():
    """Returns current timestamp in IST timezone with YYYY_MM_DD_HHMMSS format"""
    utc_now = datetime.now(pytz.utc)
    ist_timezone = pytz.timezone('Asia/Kolkata')
    return utc_now.astimezone(ist_timezone).strftime("%Y_%m_%d_%H%M%S")

with DAG(
    dag_id='with_glue_2_test',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['snowflake', 'dynamic', 'unload'],
) as dag:
    

    glue_job_1 = EmptyOperator(task_id="glue_job_1" )
    glue_job_2 = EmptyOperator(task_id="glue_job_2" )
    skip_other_rows = EmptyOperator(task_id="skip_other_rows" )

    @task.branch
    def decide_glue_job_to_run(sensor_row):
        """Choose which Glue job to run based on row count"""
        # row_count = payload['sensor_row']['TARGET_TABLE_ROW_COUNT']
        row_count = 10000001
        # if row_count <= 1000000:
        #     return "process_data_group.run_glue_job_when_low_data"
        # return "process_data_group.run_glue_job_when_Big_data"
        if row_count <= 1000000:
            print(row_count)
            return "glue_job_1"
        elif row_count > 1000000:
            print("esle if")
            return "glue_job_2"
        
        # Default/fallback if row_count is null/invalid
        return "skip_other_rows"
    

    glue_decision = decide_glue_job_to_run(1000000)
    glue_decision >> [glue_job_1, glue_job_2 , skip_other_rows]




        # glue_job_1 = GlueJobOperator(
        #     task_id="run_glue_job_when_low_data",
        #     job_name="sftp_etl_job",
        #     aws_conn_id="aws_connection_sandbox",
        #     region_name="us-east-1",
        #     script_args={
        #         '--sftp_path': 's3://sftp-etl-poc-bucket/sftp_inbound/Test_etl_new.csv'
        #         },
        #     task_group=process_data_group
        # )

        # glue_job_2 = GlueJobOperator(
        #     task_id="run_glue_job_when_Big_data",
        #     job_name="S3-Merge-job-2-HF",
        #     aws_conn_id="aws_connection_sandbox",
        #     region_name="us-east-1",
        #     script_args={
        #         '--sftp_path': 's3://sftp-etl-poc-bucket/sftp_inbound/Test_etl_new.csv'},
        #     task_group=process_data_group 
        # )

        # glue_job_1 = EmptyOperator(task_id="glue_job_1" , task_group=process_data_group)
        # glue_job_2 = EmptyOperator(task_id="glue_job_2" , task_group=process_data_group)
        # skip_other_rows = EmptyOperator(task_id="skip_other_rows" , task_group=process_data_group)
    
