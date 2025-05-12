from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from datetime import datetime, timezone
import pytz


def get_current_ist_timestamp():
    """Returns current timestamp in IST timezone with YYYY_MM_DD format"""
    # Get current time in UTC
    utc_now = datetime.now(pytz.utc)
    # Convert to IST (UTC+5:30)
    ist_timezone = pytz.timezone('Asia/Kolkata')
    ist_now = utc_now.astimezone(ist_timezone)
    # Format as YYYY_MM_DD
    return ist_now.strftime("%Y_%m_%d_%H%M%S")

with DAG(
    dag_id='dynamic_copy_unload_to_s3',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['snowflake', 'dynamic', 'unload'],
) as dag:
    
    @task
    def print_high_value():
        print("HIGH VALUE: H")
        return "H"

    @task
    def print_low_value():
        print("LOW VALUE: L")
        return "L"

    @task
    def fetch_sensor_data():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql = """
        SELECT ID, TARGET_TABLE_NAME, S3_STAGE  
        FROM BCI_POC.EXTRACT_FRAMEWORK.SENSOR_TABLE;
        """
        df = hook.get_pandas_df(sql)
        return df.to_dict(orient='records')

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
                    FROM (SELECT header FROM {menifest_table})
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
                    FROM (SELECT footer FROM {menifest_table})
                    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|')
                    OVERWRITE = TRUE HEADER = FALSE;
                """
            },
            'sensor_row': sensor_row  # Include original row data
        }

    @task
    def execute_commands(payload):
        # Extract both commands and sensor_row from the payload
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

    sensor_data = fetch_sensor_data()
    # Generate commands and include original sensor data in the return value
    high_value_task = print_high_value()
    low_value_task = print_low_value()
    command_payloads = generate_commands.expand(sensor_row=sensor_data)

    

    # Execute commands with the combined payload
    ee = execute_commands.expand(payload=command_payloads)

sensor_data >> [high_value_task, low_value_task, command_payloads] >> ee

