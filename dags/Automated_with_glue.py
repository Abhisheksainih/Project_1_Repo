from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import time
import pytz

def get_current_ist_timestamp():
    """Returns current timestamp in IST timezone with YYYY_MM_DD_HHMMSS format"""
    utc_now = datetime.now(pytz.utc)
    ist_timezone = pytz.timezone('Asia/Kolkata')
    return utc_now.astimezone(ist_timezone).strftime("%Y_%m_%d_%H%M%S")

with DAG(
    dag_id='Automated_with_glue',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['snowflake', 'dynamic', 'unload'],
) as dag:

    # Start and End markers
    start = EmptyOperator(task_id="start")
    finish_empty = EmptyOperator(task_id="finish_empty")
    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed"
    )
    # timestamp = get_current_ist_timestamp()


    @task
    def fetch_sensor_data():
        """Fetch data from Snowflake, returns list of records or empty list"""
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """
        SELECT ID, TARGET_TABLE_NAME, TARGET_TABLE_ROW_COUNT, S3_STAGE  
        FROM BCI_POC.EXTRACT_FRAMEWORK.SENSOR_TABLE;
        """
        df = hook.get_pandas_df(sql)
        return df.to_dict(orient='records')  # Returns list of dicts or empty list

    @task.branch
    def check_data_status(sensor_data):
        """Determine workflow path based on data presence"""
        if not sensor_data:  # Empty list check
            return "finish_empty"
        return "process_data_group.generate_copy_commands"


    @task
    def generate_copy_commands(sensor_row ):
        timestamp = get_current_ist_timestamp()
        id_val = sensor_row['ID']
        source_table_name = sensor_row['TARGET_TABLE_NAME']
        temp_table_path = f"{source_table_name}_{timestamp}"
        manifest_table = 'SENSOR_TABLE'
        
        s3_stage = "@my_s3_stage"
        s3_path = f"{s3_stage}/{temp_table_path}/data.gz"
        s3_path_f = f"{s3_stage}/{temp_table_path}/footer.gz"
        s3_path_h = f"{s3_stage}/{temp_table_path}/Data_folder_with_header_file/header.gz"
        
        return {
            'commands': {
                'header': f"""
                    COPY INTO {s3_path_h}
                    FROM (SELECT header FROM {manifest_table} WHERE ID = '{id_val}')
                    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|')
                    OVERWRITE = TRUE HEADER = FALSE;
                """,
                'main': f"""
                    COPY INTO {s3_path}
                    FROM {source_table_name}
                    FILE_FORMAT = (TYPE = 'CSV' COMPRESSION = 'GZIP')
                    OVERWRITE = TRUE MAX_FILE_SIZE = 5368709120;
                """,
                'footer': f"""
                    COPY INTO {s3_path_f}
                    FROM (SELECT footer FROM {manifest_table} WHERE ID = '{id_val}')
                    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|')
                    OVERWRITE = TRUE HEADER = FALSE;
                """
            },
            'sensor_row': sensor_row ,
            'execution_time': timestamp
        }

    @task
    def execute_commands_unloading(payload):
        """Execute the generated SQL commands"""
        commands = payload['commands']
        sensor_row = payload['sensor_row']
        
        SnowflakeOperator(
            task_id=f"copy_header_{sensor_row['ID']}",
            sql=commands['header'],
            snowflake_conn_id='snowflake_conn',
            dag=dag
        ).execute(context={})
        
        SnowflakeOperator(
            task_id=f"copy_main_{sensor_row['ID']}",
            sql=commands['main'],
            snowflake_conn_id='snowflake_conn',
            dag=dag
        ).execute(context={})
        
        SnowflakeOperator(
            task_id=f"copy_footer_{sensor_row['ID']}",
            sql=commands['footer'],
            snowflake_conn_id='snowflake_conn',
            dag=dag
        ).execute(context={})
        
        return payload  # Pass through the payload for downstream tasks

    @task.branch
    def decide_glue_job_to_run(payload):
        """Choose which Glue job to run based on row count"""
        row_count = payload['sensor_row']['TARGET_TABLE_ROW_COUNT']
        if row_count <= 1000000:
            return "process_glue_jobs.glue_job_1"
        elif row_count > 1000000:
            return "process_glue_jobs.glue_job_2"
        
        # Default/fallback if row_count is null/invalid
        return "process_glue_jobs.skip_other_rows"


        
    @task
    def run_glue_dispatcher(payload):
        row = payload['sensor_row']
        timestamp = payload['execution_time']
        row_count = row['TARGET_TABLE_ROW_COUNT']
        row_id = row['ID']
        source_table_name_2 = row['TARGET_TABLE_NAME']
        arg = f"{source_table_name_2}_{timestamp}"

        time.sleep(5) 

        if row_count <= 1000000:
            print(f"Running Glue Job 1 for ID: {row_id}")
            # EmptyOperator(task_id="glue_job_1" )
            # Uncomment to actually run
            GlueJobOperator(
                task_id=f"glue_job_1_{row_id}",
                job_name="S3-Merge-job-low-data",
                script_location="s3://your-script-location-small",
                aws_conn_id="aws_connection_sandbox",
                wait_for_completion=True ,
                script_args={
                '--unloaded_table_1': f"{arg}"
                },
                region_name="us-east-1"
            ).execute(context={})
        else:
            print(f"Running Glue Job 2 for ID: {row_id}")
            # EmptyOpeßrator(task_id="glue_job_2" )
            # Uncomment to actually run
            GlueJobOperator(
                task_id=f"glue_job_2_{row_id}",
                job_name="S3-Merge-job-2-HF",
                script_location="s3://your-script-location-large",
                aws_conn_id="aws_connection_sandbox",
                wait_for_completion=True ,
                script_args={
                '--unloaded_folder_path': f"bci-extracts/" , 
                '--unloaded_table': f"{arg}"
                },
                region_name="us-east-1"
            ).execute(context={})

    

    # Main workflow
    sensor_data = fetch_sensor_data()
    branch = check_data_status(sensor_data)

    # Process data branch (only created if data exists)
    with TaskGroup("process_data_group") as process_data_group:
        command_payloads = generate_copy_commands.expand(sensor_row=sensor_data)
        execution_results = execute_commands_unloading.expand(payload=command_payloads)
        glue_decision_branch = run_glue_dispatcher.expand(payload=execution_results)


        

    # Set up dependencies
    start >> sensor_data >> branch
    branch >> finish_empty
    branch >> process_data_group
######################################

    
    # Both paths should trigger end
    [process_data_group, finish_empty] >> end