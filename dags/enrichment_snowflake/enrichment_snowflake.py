from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pathlib import Path



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
        with open(sql_file_path, 'r') as file:
            sql_query = file.read().format(timestamp=start_time, table_name=table)

        df = pd.read_sql(sql_query, engine)
        df.to_csv(f'dags/enrichment_snowflake/outputs/{table}.csv', index=False, encoding='utf-8', compression=None)

def extract_fulfil_tables(tables, **kwargs):
    start_time = kwargs['ti'].xcom_pull(task_ids='extract_start_time', key='es_start_time')

    for table in tables:
        sql_file_path = f'dags/enrichment_snowflake/queries/{table}.sql'
        with open(sql_file_path, 'r') as file:
            sql_query = file.read().format(timestamp=start_time)

        df = pd.read_sql(sql_query, engine)
        df.to_csv(f'dags/enrichment_snowflake/outputs/TB_{table}.csv', index=False)


def insert_into_snowflake():
    snowflake_conn = SnowflakeHook(
        snowflake_conn_id='mako_snowflake',
        schema='ENRICHMENT'
    )
    conn = snowflake_conn.get_conn()

    directory = Path('dags/enrichment_snowflake/outputs')
    output_files = [file.stem for file in directory.glob('*.csv')]

    for table in output_files:
        print(f'Push table {table} to Snowflake')
        with conn.cursor() as cur:
            # PUT the CSV file to Snowflake stage
            put_query = f'PUT file://{directory}/{table}.csv @ENRICHMENT_STAGE/{table}'
            cur.execute(put_query)

            # COPY INTO command to load CSV data into Snowflake table
            copy_query = f'''
                            COPY INTO {table}
                            FROM @ENRICHMENT_STAGE/{table}
                            FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1);
                          '''
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

    insert_into_snowflake_task = PythonOperator(
        task_id='insert_into_snowflake',
        python_callable=insert_into_snowflake,
        # op_args=['dags/enrichment_snowflake/outputs', ['TB_Articles', 'TB_Videos', 'TB_Recipes']],
        provide_context=True,
    )

    extract_start_time_task >> [extract_main_tables_task, extract_fulfil_tables_task] >> insert_into_snowflake_task