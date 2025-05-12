from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import pytz

def get_current_ist_timestamp():
    """Returns current timestamp in IST timezone with YYYY_MM_DD_HHMMSS format"""
    utc_now = datetime.now(pytz.utc)
    ist_timezone = pytz.timezone('Asia/Kolkata')
    return utc_now.astimezone(ist_timezone).strftime("%Y_%m_%d_%H%M%S")

with DAG(
    dag_id='dynamic_unload_with_empty_table_handling',
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
    trigger_rule="none_failed" )


    @task
    def fetch_sensor_data():
        """Fetch data from Snowflake, returns list of records or empty list"""
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """
        SELECT ID, TARGET_TABLE_NAME, S3_STAGE  
        FROM BCI_POC.EXTRACT_FRAMEWORK.SENSOR_TABLE;
        """
        df = hook.get_pandas_df(sql)
        return df.to_dict(orient='records')  # Returns list of dicts or empty list

    @task.branch
    def check_data_status(sensor_data):
        """Determine workflow path based on data presence"""
        if not sensor_data:  # Empty list check
            return "finish_empty"
        return "process_data_group.generate_commands"

    @task
    def generate_commands(sensor_row):
        timestamp = get_current_ist_timestamp()
        id_val = sensor_row['ID']
        source_table_name = sensor_row['TARGET_TABLE_NAME']
        temp_table_path = sensor_row['TARGET_TABLE_NAME']
        temp_table_path = f"{temp_table_path}_{timestamp}"
        menifest_table = 'SENSOR_TABLE'
        
        s3_stage = "@my_s3_stage"
        s3_path = f"{s3_stage}/{temp_table_path}/data.gz"
        s3_path_f = f"{s3_stage}/{temp_table_path}/footer.gz"
        s3_path_h = f"{s3_stage}/{temp_table_path}/Data_folder_with_header_file/header.gz"
        
        # Return both commands and the original sensor_row
        return {
            'commands': {
                'header': f"""
                    COPY INTO {s3_path_h}
                    FROM (SELECT header FROM {menifest_table} where ID = '{id_val}')
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
                    FROM (SELECT footer FROM {menifest_table} where ID = '{id_val}')
                    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|')
                    OVERWRITE = TRUE HEADER = FALSE;
                """
            },
            'sensor_row': sensor_row  # Include original row data
        }
    # def generate_commands(sensor_row):
    #     """Generate SQL commands for each sensor row"""
    #     timestamp = get_current_ist_timestamp()
    #     temp_table_path = f"{sensor_row['TARGET_TABLE_NAME']}_{timestamp}"
    #     s3_stage = "@my_s3_stage"
        
    #     return {
    #         'commands': {
    #             'header': f"""
    #                 COPY INTO {s3_stage}/{temp_table_path}/header.gz
    #                 FROM (SELECT header FROM SENSOR_TABLE)
    #                 FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|')
    #                 OVERWRITE = TRUE HEADER = FALSE;
    #             """,
    #             'main': f"""
    #                 COPY INTO {s3_stage}/{temp_table_path}/data.gz
    #                 FROM {sensor_row['TARGET_TABLE_NAME']}
    #                 FILE_FORMAT = (TYPE = 'CSV' COMPRESSION = 'GZIP')
    #                 OVERWRITE = TRUE MAX_FILE_SIZE = 5368709120;
    #             """,
    #             'footer': f"""
    #                 COPY INTO {s3_stage}/{temp_table_path}/footer.gz
    #                 FROM (SELECT footer FROM SENSOR_TABLE)
    #                 FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|')
    #                 OVERWRITE = TRUE HEADER = FALSE;
    #             """
    #         },
    #         'sensor_row': sensor_row
    #     }

    @task
    def execute_commands(payload):
        """Execute the generated SQL commands"""
        commands = payload['commands']
        sensor_row = payload['sensor_row']
        
        # Execute header copy
        SnowflakeOperator(
            task_id=f"copy_header_{sensor_row['ID']}",
            sql=commands['header'],
            snowflake_conn_id='snowflake_conn',
            dag=dag
        ).execute(context={})
        
        # Execute main data copy
        SnowflakeOperator(
            task_id=f"copy_main_{sensor_row['ID']}",
            sql=commands['main'],
            snowflake_conn_id='snowflake_conn',
            dag=dag
        ).execute(context={})
        
        # Execute footer copy
        SnowflakeOperator(
            task_id=f"copy_footer_{sensor_row['ID']}",
            sql=commands['footer'],
            snowflake_conn_id='snowflake_conn',
            dag=dag
        ).execute(context={})

    # Main workflow
    sensor_data = fetch_sensor_data()
    branch = check_data_status(sensor_data)

    # Process data branch (only created if data exists)
    with TaskGroup("process_data_group") as process_data_group:
        command_payloads = generate_commands.expand(sensor_row=sensor_data)
        execute_commands.expand(payload=command_payloads)
    
    # Set up dependencies
    # start >> sensor_data 
    # sensor_data >> branch
    # branch >> [process_data_group, finish_empty]  # Conditional branching
    # [process_data_group, finish_empty] >> end

    # start >> sensor_data >> branch >> [process_data_group, finish_empty] >> end

    start >> sensor_data >> branch
    branch >> finish_empty
    branch >> process_data_group

    # Both paths should trigger end
    [process_data_group, finish_empty] >> end