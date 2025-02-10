import os
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from datetime import datetime, timedelta
from airflow.hooks.mssql_hook import MsSqlHook
import pandas as pd
import subprocess
import csv
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import time
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import pendulum
from airflow.models import Variable
import tempfile

select_query = '''SELECT  CAST([event_name] as nvarchar(500)) as event_name
      ,[start_time]
      ,[end_time]
      ,[duration]
      ,[duration_in_seconds]
      ,[house_id]
      ,[house_number]
      ,[event_code]
      ,[program_code]
      ,CAST([live_broadcast] as nvarchar(500)) as live_broadcast
      ,CAST([rerun_broadcast] as nvarchar(500)) as rerun_broadcast
      ,[mako_picture_web_link]
      ,CAST([event_description] as nvarchar) as event_description
  FROM [Cooladata].[dbo].[EPG_Archive]
  where start_time >= DATEADD(year, -2, GETDATE())'''


select_query_epg = '''
    select * from [KeshetTV].[dbo].[MAKO_EPG_VIEW] 
'''

select_query_mos = '''
    select * from [KNM].[dbo].[MOS_AS_RUN]
'''

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2023, 9, 4, tz='Asia/Jerusalem'),
    'email': ['tomer.man@mako.co.il'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=3),
    'provide_context': True
}

destination_hook = SnowflakeHook(snowflake_conn_id='mako_snowflake')
source_hook = MsSqlHook(mssql_conn_id='cooladata_mako')


def send_slack_error_notification(context):
    slack_token = Variable.get('slack_token')
    slack_channel = 'C05UNMWHX2R'
    
    message = f"An error occurred in the Airflow DAG '{context['dag'].dag_id}' on {context['execution_date']}.\nError Message: {context['exception']}"
    
    try:
        client = WebClient(token=slack_token)
        response = client.chat_postMessage(channel=slack_channel, text=message)
        assert response["message"]["text"] == message
    except SlackApiError as e:
        print(f"Error sending Slack message: {e.response['error']}")

def fetch_data_from_source(target_table):
    # Create a temporary stage in Snowflake for the CSV file
    stage_name = 'ENRICHAMENT_STAGE'
    csv_file = f'/home/eran.peres@keshet.prd/airflow/dags/csv/{time.time()}.csv'
    
    # Fetch data from the source SQL Server database for the last week
    k_connection = source_hook.get_conn()
    cursor = k_connection.cursor()
    cursor.execute(select_query)
    res = cursor.fetchall()
    
    # Write the data to a CSV file
    with open(csv_file, 'w', encoding='utf-8') as f:
        w = csv.writer(f, quoting=csv.QUOTE_ALL)
        w.writerows(res)

    # Use SnowflakeHook to perform MERGE INTO operation
    s_connection = destination_hook.get_conn()
    cursor = s_connection.cursor()
    
    # Create a MERGE INTO SQL statement with a condition for the last week
    merge_sql = f'''
    MERGE INTO {target_table} e
    USING (
        SELECT
            $1 AS EVENT_NAME,
            $2 AS start_time,
            $3 AS end_time,
            $4 AS duration,
            $5 AS duration_in_seconds,
            $6 AS house_id,
            $7 AS house_number,
            $8 AS event_code,
            $9 AS program_code,
            $10 AS live_broadcast,
            $11 AS rerun_broadcast,
            $12 AS mako_picture_web_link,
            $13 AS event_description
        FROM @ENRICHAMENT_STAGE/EPG (file_format => csv_format)
    ) AS source_table
    ON e.start_time = source_table.start_time
    AND source_table.start_time >= DATEADD(week, -1, GETDATE())  -- Filter for the last week
    WHEN MATCHED THEN
        UPDATE SET
            e.EVENT_NAME = source_table.EVENT_NAME,
            e.end_time = source_table.end_time,
            e.duration = source_table.duration,
            e.duration_in_seconds = source_table.duration_in_seconds,
            e.house_id = source_table.house_id,
            e.house_number = source_table.house_number,
            e.event_code = source_table.event_code,
            e.program_code = source_table.program_code,
            e.live_broadcast = source_table.live_broadcast,
            e.rerun_broadcast = source_table.rerun_broadcast,
            e.mako_picture_web_link = source_table.mako_picture_web_link,
            e.event_description = source_table.event_description
    WHEN NOT MATCHED THEN
        INSERT (
            EVENT_NAME,
            start_time,
            end_time,
            duration,
            duration_in_seconds,
            house_id,
            house_number,
            event_code,
            program_code,
            live_broadcast,
            rerun_broadcast,
            mako_picture_web_link,
            event_description
        ) VALUES (
            source_table.EVENT_NAME,
            source_table.start_time,
            source_table.end_time,
            source_table.duration,
            source_table.duration_in_seconds,
            source_table.house_id,
            source_table.house_number,
            source_table.event_code,
            source_table.program_code,
            source_table.live_broadcast,
            source_table.rerun_broadcast,
            source_table.mako_picture_web_link,
            source_table.event_description
        );
    '''

    cursor.execute(f"TRUNCATE {target_table}")
    cursor.execute(f"PUT file://{csv_file} @{stage_name}/{target_table}")
    cursor.execute(merge_sql)
    cursor.execute(f"RM @{stage_name}/{target_table}")
    s_connection.commit()
    os.remove(csv_file)
    return time.time()

    return csv_file


def EPG_to_snowflake():
    mssql_mako_hook = MsSqlHook( mssql_conn_id='makoDB_Enrichment')
    engine_mako = mssql_mako_hook.get_sqlalchemy_engine()
    engine_cooladata = source_hook.get_sqlalchemy_engine()

    def upload_dataframe(df, table):
        with tempfile.NamedTemporaryFile(suffix='.csv') as tmp_file:
            file_path = tmp_file.name
            df.to_csv(file_path, index=False)

            query = f'PUT file://{file_path} @PUBLIC.ENRICHAMENT_STAGE'
            destination_hook.run(query)

            conn = destination_hook.get_conn()
            file = os.path.basename(file_path)

            with conn.cursor() as cur:
                truncate_query = f'TRUNCATE TABLE PUBLIC.{table};'
                cur.execute(truncate_query)

                copy_query = f'''
                                COPY INTO PUBLIC.{table}
                                FROM @PUBLIC.ENRICHAMENT_STAGE/{file}
                                FILE_FORMAT = (TYPE = 'CSV' NULL_IF = (' ', 'NULL'), FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1);
                              '''
                cur.execute(copy_query)

                cur.execute(f"rm @PUBLIC.ENRICHAMENT_STAGE/{file}")

    epg = pd.read_sql(select_query_epg, engine_cooladata)
    mos = pd.read_sql(select_query_mos, engine_mako)

    upload_dataframe(epg, 'mako_epg_view')
    upload_dataframe(mos, 'mos_as_run')


with DAG('epg', schedule_interval='50 0 * * *', catchup=False, default_args=default_args) as dag:
    EPG_START = EmptyOperator(task_id="EPG_START", dag=dag)

    EPG_archive_to_snowflake = PythonOperator(
        task_id='EPG_archive_to_snowflake',
        python_callable=fetch_data_from_source,
        provide_context=True,
        op_args = ['EPG'],
        on_failure_callback=send_slack_error_notification
    )

    EPG_to_snowflake = PythonOperator(
        task_id='EPG_to_snowflake',
        python_callable=EPG_to_snowflake,
        provide_context=True,
        on_failure_callback=send_slack_error_notification
    )


    EPG_START >> EPG_archive_to_snowflake >> EPG_to_snowflake
