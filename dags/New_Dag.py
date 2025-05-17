from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
from croniter import croniter
from airflow.utils.dates import days_ago
import pytz
import logging
from dateutil.parser import parse
import json

@dag(
    dag_id="sensor_table_cron_filtering",
    schedule_interval="*/3 * * * *",  # Every 15 minutes
    start_date=days_ago(1),
    catchup=False,
    tags=["snowflake", "cron", "sensor"],
)
def extract_dispatcher():
    @task
    def fetch_due_sensor_data():
        """
        Fetch rows from SENSOR_TABLE where CRON schedule indicates they're due to run now.
        """
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sensor_sql = """
            SELECT ID, TARGET_TABLE_NAME, TARGET_TABLE_ROW_COUNT, S3_STAGE,
                EXTRACT_ID, CRON, LAST_RUN_TIME , NUMBER_OF_RUN
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
                        "ID": row["ID"],
                        "last_run": next_run.strftime("%Y-%m-%d %H:%M:%S"),  # This becomes LAST_RUN_TIME
                        "next_run": future_run.strftime("%Y-%m-%d %H:%M:%S")  # This becomes NEXT_RUN_TIME
                    })
            except Exception as e:
                logging.error(f"Error processing row {row['ID']}: {str(e)}")
                continue

        return due_rows


    @task
    def update_timestamps(row):
        """
        Update LAST_RUN_TIME to the computed last_run and NEXT_RUN_TIME to the next cron schedule.
        """
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        id = row["ID"]
        last_run = row["last_run"]  # This is the computed next_run (now being marked as completed)
        next_run = row["next_run"]  # This is the future_run (next schedule)

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
                RUN_HISTORY = PARSE_JSON('{updated_history_json}')
            WHERE ID = '{id}'
        """
        hook.run(sql)

    due_rows = fetch_due_sensor_data()
    update_timestamps.expand(row=due_rows)

dag_instance = extract_dispatcher()
