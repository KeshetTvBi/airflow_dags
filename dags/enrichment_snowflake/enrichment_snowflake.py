from venv import logger

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pathlib import Path
import re
import logging

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 1),
    'retries': 1
}

try:
    mssql_hook = MsSqlHook(
        mssql_conn_id='makoDB_Enrichment',
        schema='vcmcon',
    )

    engine = mssql_hook.get_sqlalchemy_engine()

except Exception as e:
        print(f'An unexpected error occurred: {e}')


def extract_start_time(**kwargs):
    sql_file_path = 'dags/enrichment_snowflake/queries/start_time.sql'

    with open(sql_file_path, 'r') as file:
        sql_query = file.read()

    df = pd.read_sql(sql_query, engine)

    start_time = str(df.iloc[0, 0])
    kwargs['ti'].xcom_push(key='es_start_time', value=start_time)

def extract_main_tables(tables, **kwargs):
    start_time = kwargs['ti'].xcom_pull(task_ids='extract_start_time', key='es_start_time')

    sql_file_path = 'dags/enrichment_snowflake/queries/main_tables.sql'

    for table in tables:
        log.info(f'Extract data from {table}')
        with open(sql_file_path, 'r') as file:
            sql_query = file.read().format(timestamp=start_time, table_name=table)

        chunks = []
        chunk_size = 100000

        for chunk in pd.read_sql(sql_query, engine, chunksize=chunk_size):
            # Add seconds to time values in the DISPLAYTIME column
            def fix_time_format(value):
                if pd.isnull(value) or value == '01/01/1970':
                    return None

                # Regular expression for HH:MM or HH:MM:SS formats
                pattern = r'^([0-1][0-9]|2[0-3]):([0-5][0-9])(:([0-5][0-9]))?$'
                if re.match(pattern, value):
                    value = value.replace('\n', '')
                    # print(repr(value))
                    parts = value.split(':')
                    if len(parts) == 2:  # If format is 'HH:MM'
                        return f'{value}:00'  # Append ':00'
                    if len(parts) == 3:  # Check if the value is in 'HH:MM:SS' format
                        parts[0] = parts[0].zfill(2)  # Pad single-digit hours
                        return ':'.join(parts)
                else:
                    return None

            chunk['DisplayTime'] = chunk['DisplayTime'].apply(fix_time_format)

            chunks.append(chunk)

        df = pd.concat(chunks, ignore_index=True)
        try:
            df.to_csv(f'dags/enrichment_snowflake/outputs/{table}.csv', index=False, encoding='utf-8', compression=None)
            log.info(f'Store {table} Successfully')
        except Exception as e:
            log.error(f'An error occurred: {e}')

def extract_fulfil_tables(tables, **kwargs):
    start_time = kwargs['ti'].xcom_pull(task_ids='extract_start_time', key='es_start_time')

    tables = ['publish_info']
    for table in tables:
        sql_file_path = f'dags/enrichment_snowflake/queries/{table}.sql'
        with open(sql_file_path, 'r') as file:
            sql_query = file.read().format(timestamp=start_time)

        df = pd.read_sql(sql_query, engine)

        df.to_csv(f'dags/enrichment_snowflake/outputs/TB_{table}.csv', index=False)

def extract_full_de():
    sql_file_path = f'dags/enrichment_snowflake/queries/DATA_ENRICHMENT.sql'
    with open(sql_file_path, 'r') as file:
        sql_query = file.read()

    chunk_size = 100000
    first_chunk = True

    for chunk in pd.read_sql_query(sql_query, engine, chunksize=chunk_size):
        chunk.to_csv('dags/enrichment_snowflake/outputs/DATA_ENRICHMENT.csv', mode='a', index=False, header=first_chunk)

def insert_into_snowflake():
    snowflake_conn = SnowflakeHook(
        snowflake_conn_id='mako_snowflake',
        schema='ENRICHMENT'
    )
    conn = snowflake_conn.get_conn()

    directory = Path('dags/enrichment_snowflake/outputs')
    # output_files = [file.stem for file in directory.glob('*.csv')]
    output_files = ['TB_Articles', 'tb_publish_info']

    for table in output_files:
        print(f'Push table {table} to Snowflake')
        with conn.cursor() as cur:
            # PUT the CSV file to Snowflake stage
            put_query = f'PUT file://{directory}/{table}.csv @ENRICHMENT_STAGE/{table}'
            cur.execute(put_query)

            # Truncate Table
            truncate_query = f'TRUNCATE TABLE {table};'
            cur.execute(truncate_query)

            # COPY INTO command to load CSV data into Snowflake table
            copy_query = f'''
                            COPY INTO {table}
                            FROM @ENRICHMENT_STAGE/{table}
                            FILE_FORMAT = (TYPE = 'CSV' NULL_IF = (' ', 'NULL'), FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1)
                            ON_ERROR = 'CONTINUE';
                          '''
            # ON_ERROR = 'CONTINUE'
            cur.execute(copy_query)
            print(f'Push {table} successfully to Snowflake')

            cur.execute(f"rm @ENRICHMENT_STAGE/{table}")

with (DAG(
         dag_id='enrichment_snowflake',
         default_args=default_args,
         schedule_interval=None,
         catchup=False
)  as dag):

    extract_start_time_task = PythonOperator(
        task_id='extract_start_time',
        python_callable=extract_start_time,
        provide_context=True,
        # on_failure_callback=send_slack_error_notification
    )

    extract_main_tables_task = PythonOperator(
        task_id='extract_main_tables',
        python_callable=extract_main_tables,
        op_args=[['TB_Articles', 'TB_Videos', 'TB_Recipes']],
        provide_context=True,
        # on_failure_callback=send_slack_error_notification
    )

    extract_fulfil_tables_task = PythonOperator(
        task_id='extract_fulfil_tables',
        python_callable=extract_fulfil_tables,
        op_args=[['forums_summary', 'votes_summary', 'furl', 'item_channel', 'publish_info', 'record_id', 'video_duration',
                  'vod_attribute', 'votes_summary', 'genre', 'recipe_components', 'video_creators', 'workers', 'pictures_sources',
                  'related_content', 'articles_writers', 'canonical_channel', 'channels_main_channels']],
        provide_context=True,
        # on_failure_callback=send_slack_error_notification
    )

    # extract_full_de_task = PythonOperator(
    #     task_id='extract_full_de',
    #     python_callable=extract_full_de,
    #     provide_context=True,
    #     # on_failure_callback=send_slack_error_notification
    # )

    insert_into_snowflake_task = PythonOperator(
        task_id='insert_into_snowflake',
        python_callable=insert_into_snowflake,
        # op_args=['dags/enrichment_snowflake/outputs', ['TB_Articles', 'TB_Videos', 'TB_Recipes']],
        provide_context=True,
    )


    extract_start_time_task >> [extract_main_tables_task, extract_fulfil_tables_task] >> insert_into_snowflake_task
    # extract_full_de_task >> insert_into_snowflake_task