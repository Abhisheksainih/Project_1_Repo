from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

# SQL to create a table
create_table_sql = """
CREATE OR REPLACE TABLE BCI_POC.EXTRACT_FRAMEWORK.CLAIMS_FACT_V2 (
    ID INT,
    NAME STRING,
    CREATED_AT TIMESTAMP
);
"""

with DAG(
    dag_id='create_snowflake_table',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['snowflake'],
) as dag:

    create_table = SnowflakeOperator(
        task_id='create_table',
        sql=create_table_sql,
        snowflake_conn_id='snowflake_conn',
    )
