from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context
from datetime import datetime
import time
import pytz
import logging
from croniter import croniter
from dateutil.parser import parse
import json

def get_current_ist_timestamp():
    """Returns current timestamp in IST timezone with YYYY_MM_DD_HHMMSS format"""
    utc_now = datetime.now(pytz.utc)
    ist_timezone = pytz.timezone('Asia/Kolkata')
    return utc_now.astimezone(ist_timezone).strftime("%Y_%m_%d_%H%M%S")
# def map_me(x, **context):
#     print(x)

#     from airflow.operators.python import get_current_context

#     context = get_current_context()
#     context["my_custom_template"] = f"{x}"

with DAG(
    dag_id='Automated_with_glue_v2',
    schedule_interval="*/15 * * * *",
    start_date=datetime(2023, 1, 1),
    # schedule_interval=None,
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


    # @task
    # def fetch_sensor_data():
    #     """Fetch data from Snowflake, returns list of records or empty list"""
    #     hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    #     sql = """
    #     SELECT ID, TARGET_TABLE_NAME, TARGET_TABLE_ROW_COUNT, S3_STAGE  , EXTRACT_ID
    #     FROM BCI_POC.EXTRACT_FRAMEWORK.SENSOR_TABLE;
    #     """
    #     df = hook.get_pandas_df(sql)
    #     return df.to_dict(orient='records')   # Returns list of dicts or empty list
    @task
    def fetch_sensor_data():
        """
        Fetch rows from SENSOR_TABLE where CRON schedule indicates they're due to run now.
        """
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sensor_sql = """
            SELECT ID, TARGET_TABLE_NAME, TARGET_TABLE_ROW_COUNT, S3_STAGE,
                EXTRACT_ID, CRON, LAST_RUN_TIME , NUMBER_OF_RUN , LAST_UPDATE
            FROM BCI_POC.EXTRACT_FRAMEWORK.SENSOR_TABLE
        """
        df = hook.get_pandas_df(sensor_sql)
        ist = pytz.timezone("Asia/Kolkata")
        now = datetime.now(ist)  # Keep as timezone-aware datetime
        due_rows = []

        for _, row in df.iterrows():
            cron_expr = row["CRON"]
            last_run = row["LAST_RUN_TIME"]
            logging.info(f"Processing ID={row['ID']} with CRON={cron_expr} and LAST_RUN={last_run}")

            # Parse last_run if it's a string
            if isinstance(last_run, str):
                last_run = parse(last_run)
            
            # Ensure last_run is timezone-aware (set to IST)
            if last_run.tzinfo is None:
                last_run = ist.localize(last_run)

            try:
                # Calculate next_run (first run after last_run)
                cron = croniter(cron_expr, last_run)
                next_run = cron.get_next(datetime)
                
                # Ensure next_run is timezone-aware
                if next_run.tzinfo is None:
                    next_run = ist.localize(next_run)
                
                logging.info(f"Next run: {next_run}, Now: {now}")
                
                # If next_run is in the past or now, it's due
                if next_run <= now:
                    # Calculate the NEXT_RUN_TIME (run after next_run)
                    future_run = cron.get_next(datetime)
                    if future_run.tzinfo is None:
                        future_run = ist.localize(future_run)
                    
                    due_rows.append({
                        "ID": int(row["ID"]),
                        "TARGET_TABLE_NAME": str(row["TARGET_TABLE_NAME"]),
                        "TARGET_TABLE_ROW_COUNT": int(row["TARGET_TABLE_ROW_COUNT"]),
                        "S3_STAGE": str(row["S3_STAGE"]),
                        "EXTRACT_ID": row["EXTRACT_ID"],
                        "last_run": next_run.strftime("%Y-%m-%d %H:%M:%S"),  # This becomes LAST_RUN_TIME
                        "next_run": future_run.strftime("%Y-%m-%d %H:%M:%S") 
                    })
            except Exception as e:
                logging.error(f"Error processing row {row['ID']}: {str(e)}")
                continue
        print(due_rows)

        return due_rows

    @task.branch
    def check_data_status(sensor_data):
        """Determine workflow path based on data presence"""
        if not sensor_data:  # Empty list check
            return "finish_empty"
        return "process_data_group.generate_copy_commands"
    


    @task(map_index_template="Generating Copy command for {{my_custom_template}}")
    def generate_copy_commands(sensor_row , **context):
        timestamp = get_current_ist_timestamp()
        id_val = sensor_row['ID']
        source_table_name = sensor_row['TARGET_TABLE_NAME']
        temp_table_path = f"{source_table_name}_{timestamp}"
        manifest_table = 'SENSOR_TABLE'
        context = get_current_context()
        context["my_custom_template"] = f"{source_table_name} | Extract id : {sensor_row['EXTRACT_ID']}"


        
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

    @task(map_index_template="Unloading of {{ my_custom_template }}")
    def execute_commands_unloading(payload , **context):
        """Execute the generated SQL commands"""
        commands = payload['commands']
        sensor_row = payload['sensor_row']
        source_table_name = sensor_row['TARGET_TABLE_NAME']
        context = get_current_context()
        context["my_custom_template"] = f" {source_table_name} | Extract id : {sensor_row['EXTRACT_ID']} "
        
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


        
    @task(map_index_template="Glue job{{ my_custom_template }}" , trigger_rule=TriggerRule.ALL_DONE)
    def run_glue_dispatcher(payload , **context):
        row = payload['sensor_row']
        timestamp = payload['execution_time']
        row_count = row['TARGET_TABLE_ROW_COUNT']
        row_id = row['ID']
        source_table_name_2 = row['TARGET_TABLE_NAME']
        extract_id = row['EXTRACT_ID']
        arg = f"{source_table_name_2}_{timestamp}"
        context = get_current_context()
        context["my_custom_template"] = f" trigger_for_{extract_id}"

        time.sleep(5) 

        if row_count <= 1000000:
            print(f"Running Glue Job 1 for ID: {row_id}")
            # EmptyOperator(task_id="glue_job_1" )
            # Uncomment to actually run
            context["my_custom_template"] = f" trigger_for_{extract_id} | Glue job name : S3-Merge-job-low-data "
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
            context["my_custom_template"] = f" trigger_for_{extract_id} | Glue job name : S3-Merge-job-2-HF "
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

        return payload

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def update_timestamps(payload):
        """
        Update LAST_RUN_TIME to the computed last_run and NEXT_RUN_TIME to the next cron schedule.
        """
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        row = payload['sensor_row']
        id = row["ID"]
        last_run = row["last_run"]  # This is the computed next_run (now being marked as completed)
        next_run = row["next_run"]  # This is the future_run (next schedule)
        ist = pytz.timezone("Asia/Kolkata")
        now_update = datetime.now(ist)  # Keep as timezone-aware datetime

        # Fetch current run history
        fetch_sql = f"""
            SELECT RUN_HISTORY, EXTRACT_ID
            FROM BCI_POC.EXTRACT_FRAMEWORK.SENSOR_TABLE
            WHERE ID = '{id}'
        """
        result_df = hook.get_pandas_df(fetch_sql)
        current_history = result_df["RUN_HISTORY"].iloc[0]
        extract_id = result_df["EXTRACT_ID"].iloc[0]

        # Load existing JSON or initialize empty dict
        try:
            history_dict = json.loads(current_history) if current_history else {}
        except Exception:
            history_dict = {}

        # Append new entry
        history_dict[last_run] = extract_id

        # Convert to JSON string
        updated_history_json = json.dumps(history_dict)

        
        sql = f"""
            UPDATE BCI_POC.EXTRACT_FRAMEWORK.SENSOR_TABLE
            SET 
                LAST_RUN_TIME = TO_TIMESTAMP('{last_run}'),
                NEXT_RUN_TIME = TO_TIMESTAMP('{next_run}'),
                NUMBER_OF_RUN = ( select NUMBER_OF_RUN from BCI_POC.EXTRACT_FRAMEWORK.SENSOR_TABLE where ID = '{id}' ) +1 ,
                LAST_UPDATE = '{now_update}',
                RUN_HISTORY = PARSE_JSON('{updated_history_json}')
            WHERE ID = '{id}'
        """
        hook.run(sql)

    

    # Main workflow
    sensor_data = fetch_sensor_data()
    branch = check_data_status(sensor_data)


    # Process data branch (only created if data exists)
    with TaskGroup("process_data_group") as process_data_group:
        command_payloads = generate_copy_commands.expand(sensor_row=sensor_data  )
        execution_results = execute_commands_unloading.expand(payload=command_payloads)
        glue_decision_branch = run_glue_dispatcher.expand(payload=execution_results)
        update = update_timestamps.expand(payload=glue_decision_branch)
        



    # Set up dependencies
    start >> sensor_data >> branch
    branch >> finish_empty
    branch >> process_data_group
######################################

    
    # Both paths should trigger end
    [process_data_group, finish_empty] >>  end

    
 
    