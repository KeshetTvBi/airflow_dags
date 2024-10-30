from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import timedelta
from utilities.graph_utils import graph_client
import pendulum
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import logging
import io
from pydomo import Domo
from pydomo.datasets import UpdateMethod
import csv
import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2023, 9, 4, tz='Asia/Jerusalem'),
    'email': ['tal.ugashi@keshet-d.co.il'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=3),
    'provide_context': True
}

SNOWFLAKE_CONN_ID = 'mako_snowflake'

domo = Domo(Variable.get('domoKey'), Variable.get('domoSecret'), api_host='api.domo.com')
dataset_id = 'af39caa3-5ff0-4549-9963-bd5cc69e6ba0'

g_client = graph_client(Variable.get('graph_client_id'),Variable.get('graph_client_secret') ,Variable.get('graph_tennet_id'))

remote_bind_host = 'wemdb02.keshet.prd'
remote_bind_port = 1433
# db_password = os.getenv('SQLSERVER_PASSWORD')


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


def read_file(**kwargs):
    drive_id = Variable.get('bi_team_drive_id')
    data_df = g_client.get_excel_file_data(drive_id,'ads_id_mapping.xlsx','Sheet1',data_format = 'pandas')
    data = data_df.to_dict(orient='records')
    kwargs['ti'].xcom_push(key='ads_data', value=data)


def push_to_snowflake(**kwargs):
    data_list = kwargs['ti'].xcom_pull(task_ids='read_file', key='ads_data')
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    try:
        # First, delete the existing data from the table
        delete_query = 'DELETE FROM domo_ads;'
        snowflake_hook.run(delete_query)
        logging.info('Existing data deleted from Snowflake')

        values = ', '.join(
            f"('{data['component_id']}', '{data['events_reporting_id']}', '{data['name']}', '{data['type']}', '{data['remarks']}')" for data in data_list
        )

        query = f'''
                    INSERT INTO domo_ads (component_id, events_reporting_id, name, type, remarks)
                    VALUES {values};
                '''
        snowflake_hook.run(query)
        logging.info('Batch insert completed successfully')

    except Exception as e:
        logging.error(f'Error inserting data into Snowflake: {e}')
        raise

def push_to_domo(**kwargs):
    data_list = kwargs['ti'].xcom_pull(task_ids='read_file', key='ads_data')
    s = io.StringIO()
    write = csv.DictWriter(s, fieldnames=data_list[0].keys())
    write.writerows(data_list)

    log.info(f'Push dataset to Domo')

    try:
        domo.datasets.data_import(dataset_id, s.getvalue().strip(),  update_method='REPLACE')
        logging.info('Data insert completed successfully')

    except KeyError as e:
        log.error(f'Key error occurred: {e}')
        raise

    except domo.DomoError as e:
        log.error(f'Error retrieving dataset from Domo: {e}')
        raise

    except Exception as e:
        log.error(f'An unexpected error occurred: {e}')
        raise


def push_to_sqlserver(**kwargs):
    data_list = kwargs['ti'].xcom_pull(task_ids='read_file', key='ads_data')

    try:
        mssql_hook = MsSqlHook(
            mssql_conn_id='makoDB_Enrichment',
            schema='Utilities',
            # login='enricher_user',
            # password=db_password,
        )


        # conn = mssql_hook.get_conn()
        # cursor = conn.cursor()

        # Truncate the table
        truncate_query = "TRUNCATE TABLE DomoAds;"
        mssql_hook.run(truncate_query)

        values = ', '.join(
            f"('{data['component_id']}', '{data['events_reporting_id']}', '{data['name']}', '{data['type']}', '{data['remarks']}')"
            for data in data_list
        )

        mssql_hook.run(f'''INSERT INTO DomoAds (component_id, events_reporting_id, events_name, type, remarks)
                       VALUES {values};
                       ''')
        # conn.commit()
        # cursor.close()
        # conn.close()

    except Exception as e:
        log.error(f'An unexpected error occurred: {e}')
        raise


with DAG(
        'ads_mapping',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False
) as dag:

    read_file_task = PythonOperator(
        task_id='read_file',
        python_callable=read_file,
        provide_context=True,
        on_failure_callback=send_slack_error_notification
    )

    push_to_snowflake_task = PythonOperator(
        task_id='push_to_snowflake',
        python_callable=push_to_snowflake,
        provide_context=True,
        on_failure_callback=send_slack_error_notification
    )

    push_to_domo_task = PythonOperator(
        task_id='push_to_domo',
        python_callable=push_to_domo,
        provide_context=True,
        on_failure_callback=send_slack_error_notification
    )

    push_to_sqlserver_task = PythonOperator(
        task_id='push_to_sqlserver',
        python_callable=push_to_sqlserver,
        provide_context=True,
        on_failure_callback=send_slack_error_notification
    )

    read_file_task >> [push_to_snowflake_task, push_to_domo_task, push_to_sqlserver_task]