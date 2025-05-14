from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from croniter import croniter
import snowflake.connector
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

@dag(dag_id='new_dag' , schedule_interval= None, start_date=datetime(2024, 1, 1), catchup=False)
def extract_dispatcher():

    @task()
    def get_extracts_due():
        # Replace with Snowflake connector or SnowflakeHook
        # conn = snowflake.connector.connect(...)
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        conn = hook.get_conn()
        cur = conn.cursor() 
        cur.execute("""
            SELECT extract_name, cron, last_run_time , source_table_name
            FROM extract_schedule
        """)
        now = datetime.utcnow()
        due = []
        for name, cron, last ,st in cur.fetchall():
            if croniter(cron, last).get_next(datetime) <= now:
                due.append({
                    "extract_name": name,
                    "cron": cron,
                    "last_run_time": last,
                    "source_table_name": st
                    })
        return due

    extract_names = get_extracts_due()
    print(extract_names)
    # with TaskGroup("trigger_extracts") as trigger_group:
    #     for extract_name in extract_names:
    #         s3_stage = "@my_s3_stage"
    #         temp_table_path = extract_names['source_table_name']
    #         s3_path = f"{s3_stage}/{temp_table_path}/data.gz"
    #         source_table_name = path = extract_names['source_table_name']
    #         copy_query = f"""COPY INTO {s3_path}
    #                 FROM {source_table_name}
    #                 FILE_FORMAT = (TYPE = 'CSV' COMPRESSION = 'GZIP')
    #                 OVERWRITE = TRUE ; """


    #         SnowflakeOperator(
    #                     task_id=f"unloading",
    #                     sql= copy_query,
    #                     snowflake_conn_id='snowflake_conn',
    #                     dag=dag
    #                 )

    extract_names 

extract_dispatcher_dag = extract_dispatcher()
