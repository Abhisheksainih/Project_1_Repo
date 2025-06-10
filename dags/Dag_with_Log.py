from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context
from datetime import datetime , timedelta
import time
import pytz
import logging
from croniter import croniter
from dateutil.parser import parse
import json
import random
from airflow.utils.log.logging_mixin import LoggingMixin
import re
import os
import tempfile


def get_current_ist_timestamp():
    """Returns current timestamp in IST timezone with YYYY_MM_DD_HHMMSS format"""
    utc_now = datetime.now(pytz.utc)
    ist_timezone = pytz.timezone('Asia/Kolkata')
    return utc_now.astimezone(ist_timezone).strftime("%Y_%m_%d_%H%M%S")

def log_task_status_to_snowflake(context, status, error_message=None):
    """Log task status to Snowflake mwaa_run_logs table"""
    task_instance = context['task_instance']
    dag_run = context['dag_run']
    
    # Get the task details
    run_id = dag_run.run_id
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id

    ist = pytz.timezone('Asia/Kolkata')
    start_time = task_instance.start_date.astimezone(ist) if task_instance.start_date else None
    end_time = task_instance.end_date.astimezone(ist) if task_instance.end_date else None
    duration = (task_instance.end_date - task_instance.start_date).total_seconds()
    # Format timestamps
    start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S") if start_time else "N/A"
    end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S") if end_time else "N/A"
    log_date = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")


    host_name = task_instance.hostname
    log_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")
    
    # For mapped tasks, get the original task ID and map index
    mapped_template = None
    
    if hasattr(task_instance, 'map_index') and task_instance.map_index >= 0:
        map_index = getattr(task_instance, 'map_index', None)
        mapped_template = task_instance.xcom_pull( task_ids=task_instance.task_id ,key=f"mapped_template_name_{map_index}",map_indexes=map_index)
        task_id = f"{task_id}[{task_instance.map_index}]-->[{mapped_template}]"

    ###################
    # Attempt to read the log file
    # --- Fetch Airflow UI log via REST API ---
    task_logs = ""
    try:
        # Construct the log file path dynamically based on the current task
        # log_file_path = f"/usr/local/airflow/logs/dag_id={dag_id}/run_id={run_id}/task_id={task_instance.task_id}/attempt={task_instance.try_number}.log"
        if hasattr(task_instance, 'map_index') and task_instance.map_index >= 0:
            map_index = getattr(task_instance, 'map_index', None)
            log_file_path = f"/usr/local/airflow/logs/dag_id={dag_id}/run_id={run_id}/task_id={task_instance.task_id}/map_index={map_index}/attempt={task_instance.try_number}.log"
        else:
            log_file_path = f"/usr/local/airflow/logs/dag_id={dag_id}/run_id={run_id}/task_id={task_instance.task_id}/attempt={task_instance.try_number}.log"


        
        with open(log_file_path, 'r') as file:
            task_logs = file.read()
            
        # Truncate logs if they're too long for Snowflake (16MB string limit)
        max_log_length = 32000  # Conservative limit for Snowflake
        if len(task_logs) > max_log_length:
            task_logs = task_logs[:max_log_length//2] + "\n...TRUNCATED...\n" + task_logs[-max_log_length//2:]
    except FileNotFoundError:
        task_logs = f"Log file not found at: {log_file_path}"
    except Exception as e:
        task_logs = f"Failed to read log file: {str(e)}"

    if task_logs:
        # First escape single quotes
        task_logs = task_logs.replace("'", "''")
    #     # Then escape any other problematic characters
    #     safe_task_logs = safe_task_logs.replace('\\', '\\\\')
    #     # Remove or replace any other special characters that might cause issues
    #     safe_task_logs = ''.join(char for char in safe_task_logs if ord(char) >= 32 or char in '\n\r\t')
    else:
        task_logs = None
    task_logs = f"""{task_logs}"""
    # print(f" task is --> {task_logs}")
    # print(safe_log_content)
    ############################
 
    #Get extract information from task instance (if available)
    try:
        task_info = str(task_instance.xcom_pull(task_ids=task_instance.task_id))
    except:
        task_info = None
    
    # Get extract_name and schedule_id from the task's payload if available
    extract_name = None
    schedule_id = None
    dag_start_time = None
    last_run_time = None
    check = False
    try:
        if 'payload' in task_instance.xcom_pull(task_ids=task_instance.task_id):
            payload = task_instance.xcom_pull(task_ids=task_instance.task_id)['payload']
            if 'sensor_row' in payload:
                sensor_row = payload['sensor_row']
                extract_name = sensor_row.get('EXTRACT_ID', 'Null')
                schedule_id = sensor_row.get('ID', 'Null')
                dag_start_time = sensor_row.get('dag_start_time', 'Null')
                last_run_time = sensor_row.get('last_run', 'Null')
                extract_version = 'Null'
    except:
        print("pass")
        pass
    
    # Prepare the SQL query
    safe_error_message = error_message.replace("'", "''") if error_message else None
    safe_task_info = json.dumps(task_info).replace("'", "''") if task_info else None
    if mapped_template is None:
        extract_version = None
    
    if mapped_template is not None:
        if mapped_template and "Schedule_Id" in mapped_template:
            schedule_id = re.search(r"Schedule_Id\s*:\s*(\d+)", mapped_template).group(1)
        else:
            schedule_id = None
        if mapped_template and "Extract id" in mapped_template:
            extract_name = re.search(r'Extract id\s*:\s*(\w+)', mapped_template).group(1)
        else:
            extract_name = None
        if mapped_template and "View" in mapped_template:
            extract_version = re.search(r'View\s*:\s*([^\s|]+)', mapped_template).group(1)
            extract_version = extract_version.split('_')[-1]
        else:
            extract_version = None

    sql = f"""
        INSERT INTO BCI_POC.EXTRACT_FRAMEWORK.mwaa_run_logs (
            run_id, dag_id, task_id, TASK_RETRUN_INFO, status, error_message,
            Extract_Name, SCHEDULE_ID, Dag_Start_Time, Last_Run_Time,
            duration_seconds, retry_count, host_name, task_start_time, task_end_time , Log_Insert_Date , EXTRACT_VERSION , LOG_INFO
        ) VALUES (
            '{run_id}', '{dag_id}', '{task_id}', {f"'{safe_task_info}'" if task_info else 'Null'}, '{status}',
            {f"'{safe_error_message}'" if error_message else 'Null'},
            { f"'{extract_name}'" if extract_name else 'Null' }, 
            {f"'{schedule_id}'" if schedule_id else 'Null' },
            {f"'{dag_start_time}'" if  dag_start_time else 'Null'}, 
            {f"'{last_run_time}'" if last_run_time else 'Null'},
            {duration}, {task_instance.try_number}, '{host_name}', '{start_time_str}', '{end_time_str}' , '{log_date}',
            {f"'{extract_version}'" if extract_version  else 'Null'} ,
            {f"'{task_logs}'" if task_logs else 'Null'}
        )
    """
    
    # Execute the query
    try:
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        hook.run(sql)
    except Exception as e:
        logging.error(f"Failed to log task status to Snowflake: {str(e)}")

def success_callback(context):
    """Callback for successful task execution"""
    logging.info("SUCCESS CALLBACK TRIGGERED")  # Check for this in logs
    log_task_status_to_snowflake(context, 'SUCCESS')

def failure_callback(context):
    """Callback for failed task execution"""
    error_message = str(context.get('exception') or "Unknown error")
    log_task_status_to_snowflake(context, 'FAILED', error_message)

default_args = {
    'owner': 'airflow',
    'on_success_callback': success_callback,
    'on_failure_callback': failure_callback
}

    
with DAG(
    dag_id='Extraction_framework_dag_with_log_v4',
    # schedule_interval="*/15 * * * *",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args, 
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
    # on_success_callback= success_callback, on_failure_callback= failure_callback
    @task(on_success_callback= success_callback, on_failure_callback= failure_callback)
    def fetch_sensor_data():
        """
        Fetch rows from SENSOR_TABLE where CRON schedule indicates they're due to run now.
        """
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        # sensor_sql = """
        #     SELECT ID, TARGET_TABLE_NAME, TARGET_TABLE_ROW_COUNT, S3_STAGE,
        #         EXTRACT_ID, CRON, LAST_RUN_TIME , NUMBER_OF_RUN , LAST_UPDATE
        #     FROM BCI_POC.EXTRACT_FRAMEWORK.SENSOR_TABLE
        # """
        sensor_sql = """
        SELECT SCHEDULE_ID as ID, 
        VIEW_NAME as TARGET_TABLE_NAME, 
        ROW_COUNT as TARGET_TABLE_ROW_COUNT, null as S3_STAGE,
        COALESCE(
            NULLIF(f.value:"Overwrite Value":"2"::string, ''),
            f.value:"Default Value":"2"::string
            ) AS     EXTRACT_ID,  CRONJOB as CRON, LAST_RUN_TIME
        FROM BCI_POC.EXTRACT_FRAMEWORK.EXTRACT_SCHEDULE e ,
        LATERAL FLATTEN(input => e.state) f
        WHERE f.key ILIKE '%param_table'
        and IS_ACTIVE = TRUE and CRONJOB is not null and  CRONJOB <> ''
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
            
            if last_run is None:
                # last_run = datetime.now()
                last_run = datetime.now(ist) 
                cron = croniter(cron_expr, last_run)
                last_run = cron.get_prev(datetime)
                last_run = cron.get_prev(datetime)
            
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
                        "next_run": future_run.strftime("%Y-%m-%d %H:%M:%S"),
                        "dag_start_time" : now.strftime("%Y-%m-%d %H:%M:%S")
                    })
            except Exception as e:
                logging.error(f"Error processing row {row['ID']}: {str(e)}")
                continue
        # print(due_rows)

        return due_rows

    @task.branch
    def check_data_status(sensor_data):
        """Determine workflow path based on data presence"""
        if not sensor_data:  # Empty list check
            return "finish_empty"
        return "process_data_group.generate_copy_commands"
    


    @task(map_index_template="Generating Copy command for View : {{my_custom_template}}" )
    def generate_copy_commands(sensor_row , **context):
        timestamp = get_current_ist_timestamp()
        id_val = sensor_row['ID']
        source_table_name = sensor_row['TARGET_TABLE_NAME']
        temp_table_path = f"{source_table_name}_{timestamp}"
        # manifest_table = 'SENSOR_TABLE'
        manifest_table = 'BCI_POC.EXTRACT_FRAMEWORK.EXTRACT_SCHEDULE '
        context = get_current_context()
        context["my_custom_template"] = f"{source_table_name} | Extract id : {sensor_row['EXTRACT_ID']} | Schedule_Id : {sensor_row['ID']} "
        ####################
        my_template = f"Generating Copy command for View : {source_table_name} | Extract id : {sensor_row['EXTRACT_ID'] } | Schedule_Id : {sensor_row['ID']}"
        ti = context["ti"]
        map_index = getattr(ti, "map_index", None)
        if map_index is not None:
            ti.xcom_push(key=f"mapped_template_name_{map_index}", value=my_template)
        else:
            ti.xcom_push(key="mapped_template_name_0", value=my_template)
        ####################
        
        s3_stage = "@my_s3_stage"
        s3_path = f"{s3_stage}/{temp_table_path}/data.gz"
        s3_path_f = f"{s3_stage}/{temp_table_path}/footer.gz"
        s3_path_h = f"{s3_stage}/{temp_table_path}/Data_folder_with_header_file/header.gz"
        
        return {
            'commands': {
                'header': f"""
                    COPY INTO {s3_path_h}
                    FROM (SELECT header FROM {manifest_table} WHERE SCHEDULE_ID = '{id_val}')
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
                    FROM (SELECT footer FROM {manifest_table} WHERE SCHEDULE_ID = '{id_val}')
                    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|')
                    OVERWRITE = TRUE HEADER = FALSE;
                """
            },
            'sensor_row': sensor_row ,
            'execution_time': timestamp
        }

    @task(map_index_template="Unloading of View : {{ my_custom_template }}")
    def execute_commands_unloading(payload , **context):
        """Execute the generated SQL commands"""
        commands = payload['commands']
        sensor_row = payload['sensor_row']
        source_table_name = sensor_row['TARGET_TABLE_NAME']
        context = get_current_context()
        context["my_custom_template"] = f" {source_table_name} | Extract id : {sensor_row['EXTRACT_ID']} "

        #########################
        my_template = f"Unloading of View : {source_table_name} | Extract id : {sensor_row['EXTRACT_ID'] } | Schedule_Id : {sensor_row['ID']}"
        ti = context["ti"]
        map_index = getattr(ti, "map_index", None)
        if map_index is not None:
            ti.xcom_push(key=f"mapped_template_name_{map_index}", value=my_template)
        else:
            ti.xcom_push(key="mapped_template_name_0", value=my_template)
        ##########################
        
        SnowflakeOperator(
            task_id=f"copy_header_{sensor_row['ID']}",
            sql=commands['header'],
            snowflake_conn_id='snowflake_conn',
            dag=dag,
            on_success_callback=success_callback,
            on_failure_callback=failure_callback
        ).execute(context={})
        
        SnowflakeOperator(
            task_id=f"copy_main_{sensor_row['ID']}",
            sql=commands['main'],
            snowflake_conn_id='snowflake_conn',
            dag=dag,
            on_success_callback=success_callback,
            on_failure_callback=failure_callback
        ).execute(context={})
        
        SnowflakeOperator(
            task_id=f"copy_footer_{sensor_row['ID']}",
            sql=commands['footer'],
            snowflake_conn_id='snowflake_conn',
            dag=dag,
            on_success_callback=success_callback,
            on_failure_callback=failure_callback
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
            #######################
            my_template = f"Glue job trigger for View : {source_table_name_2} | Extract id : {extract_id}|  Schedule_Id : {row_id} | Glue job name : S3-Merge-job-low-data "
            ti = context["ti"]
            map_index = getattr(ti, "map_index", None)
            if map_index is not None:
                ti.xcom_push(key=f"mapped_template_name_{map_index}", value=my_template)
            else:
                ti.xcom_push(key="mapped_template_name_0", value=my_template)
            #######################
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
            #####################
            my_template = f"Glue job trigger for View : {source_table_name_2} | Extract id : {extract_id}|  Schedule_Id : {row_id} | Glue job name : S3-Merge-job-2-HF "
            ti = context["ti"]
            map_index = getattr(ti, "map_index", None)
            if map_index is not None:
                ti.xcom_push(key=f"mapped_template_name_{map_index}", value=my_template)
            else:
                ti.xcom_push(key="mapped_template_name_0", value=my_template)
            #####################
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
        print(payload)

        return payload

    @task(trigger_rule=TriggerRule.ALL_DONE , map_index_template="Update Last/Next run time for Id:{{ my_custom_template }}" )
    def update_last_next_run(payload , **context):
        """
        Update LAST_RUN_TIME to the computed last_run and NEXT_RUN_TIME to the next cron schedule.
        """
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        row = payload['sensor_row']
        id = row["ID"]
        last_run = row["last_run"]  # This is the computed next_run (now being marked as completed)
        next_run = row["next_run"]  # This is the future_run (next schedule)
        ist = pytz.timezone("Asia/Kolkata")
        eid = row["EXTRACT_ID"]
        now_update = datetime.now(ist)  # Keep as timezone-aware datetime
        context = get_current_context()
        context["my_custom_template"] = f" trigger_for_{eid}"
        #########################
        my_template = f"Update Last/Next run time for View : {row['TARGET_TABLE_NAME']} | Extract id : {row['EXTRACT_ID'] } | Schedule_Id : {row['ID']}"
        ti = context["ti"]
        map_index = getattr(ti, "map_index", None)
        if map_index is not None:
            ti.xcom_push(key=f"mapped_template_name_{map_index}", value=my_template)
        else:
            ti.xcom_push(key="mapped_template_name_0", value=my_template)
        ##########################
        
        Dag_time = row['dag_start_time']    
        sql = f"""
            UPDATE BCI_POC.EXTRACT_FRAMEWORK.EXTRACT_SCHEDULE
            SET 
                LAST_RUN_TIME = TO_TIMESTAMP('{last_run}'),
                NEXT_RUN_TIME = TO_TIMESTAMP('{next_run}')
            WHERE SCHEDULE_ID = '{id}' and IS_ACTIVE = TRUE
        """
        hook.run(sql)
        dag_run = context['dag_run']
        run_id = dag_run.run_id

        sql_update_autid_log = f"""  insert into BCI_POC.EXTRACT_FRAMEWORK.mwaa_audit_logs(Extract_Name , SCHEDULE_ID,
        Dag_Start_Time , Last_Run_Time , Log_Insert_Date , run_id  ) 
        values(
        '{eid}' ,
        '{id}' ,
        '{Dag_time}',
        '{last_run}' ,
        '{now_update}' , '{run_id}'  ) """

        hook.run(sql_update_autid_log)

    # Main workflow
    sensor_data = fetch_sensor_data()
    branch = check_data_status(sensor_data)

    # Process data branch (only created if data exists)
    with TaskGroup("process_data_group") as process_data_group:
        command_payloads = generate_copy_commands.expand(sensor_row=sensor_data  )
        execution_results = execute_commands_unloading.expand(payload=command_payloads)
        glue_decision_branch = run_glue_dispatcher.expand(payload=execution_results)
        update = update_last_next_run.partial(trigger_rule=TriggerRule.ALL_DONE).expand(payload=glue_decision_branch)
        


    # Set up dependencies
    start >> sensor_data >> branch
    branch >> finish_empty
    branch >> process_data_group
######################################

    # Both paths should trigger end
    [process_data_group, finish_empty] >>  end


    ######test comit ########## ########
    #########

    
 
    