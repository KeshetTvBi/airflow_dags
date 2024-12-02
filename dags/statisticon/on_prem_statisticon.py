from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import re
# import paramiko
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import time
import subprocess
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import os
import shutil
import csv
import json


env = os.getenv('ENV')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 1),
    'retries': 1
}

def send_slack_error_notification(context):
    slack_token = '****'
    slack_channel = 'C05UNMWHX2R'

    message = f"An error occurred in the Airflow DAG '{context['dag'].dag_id}' on {context['execution_date']}.\nError Message: {context['exception']}"

    try:
        client = WebClient(token=slack_token)
        response = client.chat_postMessage(channel=slack_channel, text=message)
        assert response["message"]["text"] == message
    except SlackApiError as e:
        print(f"Error sending Slack message: {e.response['error']}")


def extract_data(**kwargs):
    try:
        # MsSqlHook connection parameters
        mssql_hook = MsSqlHook(
            mssql_conn_id='makoDB_Enrichment',
            schema='KNM_PRD'
        )

        engine = mssql_hook.get_sqlalchemy_engine()

        ### Full Version ###
        sql_full_path = 'dags/statisticon/queries/full_version.sql'

        with open(sql_full_path, 'r') as file:
            sql_query = file.read()

        df = pd.read_sql(sql_query, engine)
        df.to_csv('dags/statisticon/tmp/full_version.csv', index=False)

        ### Reset Version ###
        sql_reset_path = 'dags/statisticon/queries/reset_version.sql'

        with open(sql_reset_path, 'r') as file:
            sql_query = file.read()

        df = pd.read_sql(sql_query, engine)
        df.to_csv('dags/statisticon/tmp/reset_version.csv', index=False)

    except Exception as e:
        print(f'An unexpected error occurred: {e}')


def transform_data(**kwargs):
    df_full = pd.read_csv('dags/statisticon/tmp/full_version.csv')
    df_reset = pd.read_csv('dags/statisticon/tmp/reset_version.csv')

    df_full['FirstClickTimestamp'] = df_full['FirstClickTimestamp'].str.replace(r'\.\d+', '', regex=True)
    df_full['FirstClickTimestamp'] = pd.to_datetime(df_full['FirstClickTimestamp'])

    df_reset['FirstClickTimestamp'] = df_reset['FirstClickTimestamp'].str.replace(r'\.\d+', '', regex=True)
    df_reset['FirstClickTimestamp'] = pd.to_datetime(df_reset['FirstClickTimestamp'])

    today = pd.Timestamp.now(tz='Asia/Jerusalem').floor('S').tz_localize(None)
    two_days_ago = today - pd.Timedelta(days=2)

    # Filter the DataFrame for the last two days
    df_full = df_full[(df_full['FirstClickTimestamp'] >= two_days_ago)]
    df_reset = df_reset[(df_reset['FirstClickTimestamp'] >= two_days_ago)]

    def process_url(url):
        # Extract the URL up to '?'
        result = re.match(r'(http://[^\?]+)', url)
        friendly_url = result.group(1).replace('http', 'https', 1) if result else None

        # Extract the pId value
        p_id_match = re.search(r'pId=(\d+)', url)
        p_id = p_id_match.group(1) if p_id_match else None

        return pd.Series([friendly_url, p_id])

    df_full_grouped = df_full.groupby('URL').agg({'FirstClickTimestamp': 'min', 'LastClickTimestamp': 'max', 'MaxMinuteAvg': 'max',
                                        'ClickCounter': 'sum', 'TotalCountWeb': 'sum', 'TotalCountMobile': 'sum',
                                        'TotalCountMobileApp': 'sum', 'TotalCountWinApp': 'sum'}).reset_index()

    df_full_dims = df_full[['URL', 'LastClickTimestamp', 'ID', 'URLHashCode', 'MaxAvgTimestamp', 'Channel', 'PortletId']]

    df_full = pd.merge(df_full_grouped, df_full_dims, how='left', on=['URL', 'LastClickTimestamp'])

    # Apply the function to the 'URL' column
    df_full[['friendly_url', 'p_id']] = df_full['URL'].apply(process_url)
    df_reset[['friendly_url', 'p_id']] = df_reset['URL'].apply(process_url)

    col_order = ['ID', 'URL', 'Channel', 'FirstClickTimestamp', 'LastClickTimestamp', 'MaxMinuteAvg', 'MaxAvgTimestamp',
                 'ClickCounter', 'PortletId', 'URLHashCode', 'TotalCountWeb', 'TotalCountMobile', 'TotalCountMobileApp',
                 'TotalCountWinApp', 'friendly_url', 'p_id']

    df_full = df_full[col_order]
    df_reset = df_reset[col_order]

    df_full.to_csv('dags/statisticon/tmp/full_version_output.csv',  index=False)
    df_reset.to_csv('dags/statisticon/tmp/reset_version_output.csv', index=False)


def csv_to_domo():
    if env == 'dev':
        subprocess.run(['java', '-jar', 'domoUtil.jar', '-s', 'pushscript_dev.script'], cwd='/opt/airflow/dags/utilities')
    else:
        subprocess.run(['java', '-jar', '/home/domoUtil/domoUtil.jar', '-s', 'dags/statisticon/pushscript.script'])


def run_dataset_domo(**kwargs):
    if env == 'dev':
        subprocess.run(['java', '-jar', 'domoUtil.jar', '-s', 'run_dataset.script'],
                       cwd='/opt/airflow/dags/utilities')
    else:
        subprocess.run(['java', '-jar', '/home/domoUtil/domoUtil.jar', '-s', 'dags/statisticon/run_dataset.script'])

def cleanup_files():
    print(f'Remove all files from the temporary directory')
    tmp_path = 'dags/statisticon/tmp'
    if os.path.exists(tmp_path):
        shutil.rmtree(tmp_path)  # Remove the directory and all its contents
        os.makedirs(tmp_path)  # Recreate the directory

    else:
        print(f'Directory {tmp_path} does not exist')


with (DAG(
         dag_id='statisticon',
         default_args=default_args,
         schedule_interval='*/3 * * * *',
         # schedule_interval=None,
         catchup=False
)  as dag):

    # Task to read the CSV file
    extract_data_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True,
        # on_failure_callback=send_slack_error_notification
    )

    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
        # on_failure_callback=send_slack_error_notification
    )

    csv_to_domo_task = PythonOperator(
        task_id='csv_to_domo',
        python_callable=csv_to_domo,
        provide_context=True,
        # on_failure_callback=send_slack_error_notification
    )

    run_dataset_domo_task = PythonOperator(
        task_id='run_dataset_domo',
        python_callable=run_dataset_domo,
        provide_context=True,
        # on_failure_callback=send_slack_error_notification
    )

    # get_streamid_domo_task = PythonOperator(
    #     task_id='get_streamid_domo',
    #     python_callable=get_streamid_domo,
    #     provide_context=True,
    #     # on_failure_callback=send_slack_error_notification
    # )

    # extract_streamid_task = PythonOperator(
    #     task_id='extract_streamid',
    #     python_callable=extract_streamid,
    #     provide_context=True,
    #     # on_failure_callback=send_slack_error_notification
    # )

    cleanup_files_task = PythonOperator(
        task_id='cleanup_files',
        python_callable=cleanup_files
    )

    # Define task dependencies
    extract_data_task >> transform_data_task >> csv_to_domo_task >> run_dataset_domo_task >> cleanup_files_task
