from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import re
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from sqlalchemy import create_engine
import time
import subprocess
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import os
import shutil
import csv


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 1),
    'retries': 1
}

def extract_data(**kwargs):
    try:
        mssql_hook = MsSqlHook(
            mssql_conn_id='makoDB_Enrichment',
            schema='vcmcon',
        )

        engine = mssql_hook.get_sqlalchemy_engine()

        sql_file_path = 'dags/enrichment_snowflake/queries/articles.sql'

        with open(sql_file_path, 'r') as file:
            sql_query = file.read()

        df = pd.read_sql(sql_query, engine)
        df.to_csv('dags/enrichment_snowflake/outputs/articles.csv', index=False)

    except Exception as e:
        print(f'An unexpected error occurred: {e}')


def insert_into_snowflake(filepath, table):
    snowflake_conn = SnowflakeHook(
        snowflake_conn_id='mako_snowflake',
        schema='ENRICHMENT'
    )
    conn = snowflake_conn.get_conn()

    with conn.cursor() as cur:
        # PUT the CSV file to Snowflake stage
        put_query = f'PUT file://{filepath} @ENRICHMENT_STAGE/{table}'
        cur.execute(put_query)

        # COPY INTO command to load CSV data into Snowflake table
        copy_query = f'''
                        COPY INTO {table}
                        FROM @ENRICHMENT_STAGE/{table}
                        FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1);
                      '''
        cur.execute(copy_query)

        cur.execute(f"rm @ENRICHMENT_STAGE/{table}")

with DAG(
         dag_id='enrichment_snowflake',
         default_args=default_args,
         schedule_interval=None,
         catchup=False
)  as dag:

    # Task to read the CSV file
    extract_data_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True,
        # on_failure_callback=send_slack_error_notification
    )

    insert_into_snowflake_task = PythonOperator(
        task_id='insert_into_snowflake',
        python_callable=insert_into_snowflake,
        op_args=['dags/enrichment_snowflake/outputs/articles.csv', 'articles'],
        provide_context=True,
    )

    extract_data_task >> insert_into_snowflake_task