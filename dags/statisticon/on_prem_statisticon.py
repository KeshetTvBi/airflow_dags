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


# SSH connection details
# ssh_host = '10.0.4.16'
# ssh_user = 'tal.ugashi'
# ssh_password = '****'
# ssh_port = 22

# SQL Server connection details
# remote_bind_host = 'wemdb02.keshet.prd'
# remote_bind_port = 1433
# local_bind_port = 14330  # Local port for SSH tunneling
# db_password = os.getenv('SQLSERVER_PASSWORD')

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


# Create SSH tunnel
# def create_ssh_tunnel():
#     client = paramiko.SSHClient()
#     client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#     client.connect(ssh_host, username=ssh_user, password=ssh_password, port=ssh_port)
#
#     transport = client.get_transport()
#     local_forward = transport.open_channel(
#         'direct-tcpip',
#         (remote_bind_host, remote_bind_port),
#         ('localhost', local_bind_port)
#     )
#
#     return client, local_forward

def extract_data(**kwargs):
    # ssh_client, tunnel = create_ssh_tunnel()

    try:
        # Give the tunnel some time to establish
        time.sleep(2)

        # MsSqlHook connection parameters
        mssql_hook = MsSqlHook(
            mssql_conn_id='makoDB_Enrichment',
            # host=remote_bind_host,
            # port=remote_bind_port,
            schema='KNM_PRD',
            # login='enricher_user',
            # password=db_password,
        )

        # Test connection or run queries
        conn = mssql_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute('select * from UrlClickCounters')
        rows = cursor.fetchall()
        # print(f"Query result: {rows}")

        # Convert rows to a list of dictionaries
        data = [dict(zip([col[0] for col in cursor.description], row)) for row in rows]

        # Specify the CSV file name
        filename = 'dags/statisticon/tmp/result.csv'

        # Write to CSV
        with open(filename, "w", newline="") as file:
            # Extract field names from the first dictionary
            fieldnames = data[0].keys()

            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

    except Exception as e:
        print(f'An unexpected error occurred: {e}')
    # finally:
        # Close SSH tunnel
        # tunnel.close()
        # ssh_client.close()


def transform_data(**kwargs):
    # Pull the query result from XCom
    # query_result = kwargs['ti'].xcom_pull(task_ids='query_sql_server', key='query_result')

    # ti = kwargs['ti']
    # data = ti.xcom_pull(task_ids='extract_data')
    # df = pd.DataFrame(data)
    df = pd.read_csv('dags/statisticon/tmp/result.csv')

    df['FirstClickTimestamp'] = df['FirstClickTimestamp'].str.replace(r'\.\d+', '', regex=True)
    df['FirstClickTimestamp'] = pd.to_datetime(df['FirstClickTimestamp'])
    # df['LastClickTimestamp'] = df['LastClickTimestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    # df['MaxAvgTimestamp'] = df['MaxAvgTimestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

    today = pd.Timestamp.now(tz='Asia/Jerusalem').floor('S').tz_localize(None)
    two_days_ago = today - pd.Timedelta(days=2)

    # Filter the DataFrame for the last two days
    df = df[(df['FirstClickTimestamp'] >= two_days_ago)]

    def process_url(url):
        # Extract the URL up to '?'
        result = re.match(r'(http://[^\?]+)', url)
        friendly_url = result.group(1).replace('http', 'https', 1) if result else None

        # Extract the pId value
        p_id_match = re.search(r'pId=(\d+)', url)
        p_id = p_id_match.group(1) if p_id_match else None

        return pd.Series([friendly_url, p_id])

    # Due to reset URL is not unique, so I'll group by DF
    # group_columns = [col for col in df.columns if col not in ['FirstClickTimestamp', 'LastClickTimestamp', 'MaxMinuteAvg',
    #                                                           'ClickCounter', 'TotalCountWeb', 'TotalCountMobile',
    #                                                           'TotalCountMobileApp', 'TotalCountWinApp']]

    df_grouped = df.groupby('URL').agg({'FirstClickTimestamp': 'min', 'LastClickTimestamp': 'max', 'MaxMinuteAvg': 'max',
                                        'ClickCounter': 'sum', 'TotalCountWeb': 'sum', 'TotalCountMobile': 'sum',
                                        'TotalCountMobileApp': 'sum', 'TotalCountWinApp': 'sum'}).reset_index()
    print("Number of rows df_grouped:", len(df_grouped))
    df_dims = df[['URL', 'LastClickTimestamp', 'ID', 'URLHashCode', 'MaxAvgTimestamp', 'Channel', 'PortletId']]

    df = pd.merge(df_grouped, df_dims, how='left', on=['URL', 'LastClickTimestamp'])
    print("Number of rows df:", len(df))
    # Apply the function to the 'URL' column
    df[['friendly_url', 'p_id']] = df['URL'].apply(process_url)

    col_order = ['ID', 'URL', 'Channel', 'FirstClickTimestamp', 'LastClickTimestamp', 'MaxMinuteAvg', 'MaxAvgTimestamp',
                 'ClickCounter', 'PortletId', 'URLHashCode', 'TotalCountWeb', 'TotalCountMobile', 'TotalCountMobileApp',
                 'TotalCountWinApp', 'friendly_url', 'p_id']

    df = df[col_order]

    df.to_csv('dags/statisticon/tmp/output.csv',  index=False)


def csv_to_domo():
    # subprocess.run(['java', '-jar', 'domoUtil.jar', '-s', 'pushscript.script'], cwd='/opt/airflow/dags/statisticon')

    # Prod
    subprocess.run(['java', '-jar', '/home/domoUtil/domoUtil.jar', '-s', 'dags/statisticon/pushscript.script'])



# def get_streamid_domo():
#     subprocess.run(['java', '-jar', 'domoUtil.jar', '-s', 'get_streamid.script'], cwd='/opt/airflow/dags/statisticon')

    # Prod
    # subprocess.run(['java', '-jar', '/home/domoUtil/domoUtil.jar', '-s', 'dags/statisticon/pushscript.script'])


# def extract_streamid(**kwargs):
#     with open('dags/statisticon/tmp/dataset.json', 'r') as file:
#         data = json.load(file)
#
#     stream_id = data.get('streamId')
#
#     if stream_id:
#         print('Stream ID:', stream_id)
#
#     else:
#         print("'streamId' key not found in the JSON data.")
#
#     kwargs['ti'].xcom_push(key='statisticon_streamid', value=stream_id)


def run_dataset_domo(**kwargs):
    # streamid = kwargs['ti'].xcom_pull(task_ids='extract_streamid', key='statisticon_streamid')

    # subprocess.run(['java', '-jar', 'domoUtil.jar', '-s', 'run_dataset.script'],
    #                cwd='/opt/airflow/dags/statisticon')

    # Prod
    subprocess.run(['java', '-jar', '/home/domoUtil/domoUtil.jar', '-s', 'dags/statisticon/run_dataset.script'])

def cleanup_files():
    print(f'Remove all files from the temporary directory')
    tmp_path = 'dags/statisticon/tmp'
    if os.path.exists(tmp_path):
        shutil.rmtree(tmp_path)  # Remove the directory and all its contents
        os.makedirs(tmp_path)  # Recreate the directory

    else:
        print(f'Directory {tmp_path} does not exist')


with DAG(
         dag_id='statisticon',
         default_args=default_args,
         schedule_interval='*/3 * * * *',
         catchup=False
)  as dag:

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

    # get_streamid_domo_task >> extract_streamid_task >> run_dataset_domo_task >> cleanup_files_task
