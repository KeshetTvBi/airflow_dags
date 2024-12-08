from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
import pandas as pd
import traceback
import pendulum
import datetime
from datetime import timedelta
from googleads import ad_manager
from googleads import errors
import _locale
from pydomo import Domo
import subprocess
from ad_manager.client_api import Client
import logging
import os


env = os.getenv('ENV')

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

domo = Domo(Variable.get('domoKey'), Variable.get('domoSecret'), api_host='api.domo.com')
dfp_dataset_id = '4e35d12f-8061-4d3b-b726-efc7728f603c'

SNOWFLAKE_CONN_ID = 'mako_snowflake'

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

def create_campaign_report():
    fields = [x['name'] for x in domo.datasets.get(dfp_dataset_id)['schema']['columns']]

    now = datetime.datetime.today()
    from_date = now - datetime.timedelta(days=1)

    weeks = [(from_date + datetime.timedelta(days=i), from_date + datetime.timedelta(days=i + 6)) for i in
             range(0, (now - from_date).days, 7)]
    weeks.reverse()
    dfp_report_data_final = []

    for f_date, t_date in weeks:
        if t_date > datetime.datetime.today():
            t_date = datetime.datetime.today()

        nowObj = {
            'year': t_date.year,
            'month': t_date.month,
            'day': t_date.day
        }
        weekagoObj = {
            'year': f_date.year,
            'month': f_date.month,
            'day': f_date.day
        }

        log.info(f'Display report for {f_date.date()} - {t_date.date()}')

        log.info(f'Initialize the client')
        client = ad_manager.AdManagerClient.LoadFromStorage('dags/ad_manager/googleads.yaml')

        client_api = Client(client)
        dfp_report_data = client_api.download_report(13653086304, weekagoObj, nowObj)

        dfp_report_data_final = dfp_report_data_final + [
            {key.replace('Dimension.', '').replace('DimensionAttribute.', '').replace('Column.', '').lower(): (
                value if value != 'N/A' else None) for
             key, value in row.items()} for row in dfp_report_data]

    dfp_report_data_final = [{k: x[k] for k in fields} for x in dfp_report_data_final]

    df_final = pd.DataFrame(dfp_report_data_final)
    df_final.to_csv('dags/ad_manager/tmp/campaign.csv', index=False)


def create_display_report():
    today = datetime.datetime.today()
    end_date = {
        "year": today.year,
        "month": today.month,
        "day": today.day
    }
    yesterday = today - datetime.timedelta(days=1)

    start_date = {
        "year": yesterday.year,
        "month": yesterday.month,
        "day": yesterday.day
    }

    log.info(f'Display report for {end_date} - {start_date}')

    log.info(f'Initialize the client')
    client = ad_manager.AdManagerClient.LoadFromStorage('dags/ad_manager/googleads.yaml')

    client_api = Client(client)
    dfp_report_data = client_api.download_report(15108503905, start_date, end_date)
    # dfp_report_data = client_api.download_report(13653086304, start_date, end_date)

    df_final = pd.DataFrame(dfp_report_data)
    df_final.to_csv('dags/ad_manager/tmp/display.csv', index=False)


def push_to_snowflake(**kwargs):
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    df = pd.read_csv('dags/ad_manager/tmp/display.csv')

    data_list = df.to_dict(orient='records')
    try:
        values = ', '.join(
            f"('{data['Dimension.ORDER_NAME']}', '{data['Dimension.LINE_ITEM_NAME']}', '{data['Dimension.CREATIVE_NAME']}', "
            f"'{data['Dimension.LINE_ITEM_TYPE']}', '{data['Dimension.ADVERTISER_NAME']}', '{data['Dimension.ORDER_ID']}', "
            f"'{data['Dimension.LINE_ITEM_ID']}', '{data['Dimension.CREATIVE_ID']}', '{data['Dimension.ADVERTISER_ID']}', "
            f"'{data['DimensionAttribute.LINE_ITEM_FREQUENCY_CAP']}', '{data['DimensionAttribute.LINE_ITEM_GOAL_QUANTITY']}', "
            f"'{data['DimensionAttribute.LINE_ITEM_END_DATE_TIME']}', '{data['DimensionAttribute.LINE_ITEM_START_DATE_TIME']}', "
            f"'{data['DimensionAttribute.LINE_ITEM_COST_PER_UNIT']}', '{data['DimensionAttribute.LINE_ITEM_COMPUTED_STATUS']}', "
            f"'{data['DimensionAttribute.LINE_ITEM_DISCOUNT']}', '{data['DimensionAttribute.ORDER_SALESPERSON']}', "
            f"'{data['DimensionAttribute.ORDER_PO_NUMBER']}', '{data['Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS']}', "
            f"'{data['Column.TOTAL_LINE_ITEM_LEVEL_CLICKS']}', '{data['Column.TOTAL_LINE_ITEM_LEVEL_CTR']}', "
            f"'{data['Column.TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE']}', '{data['Column.TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS_RATE']}'))" for data in data_list
        )

        query = f'''
                    INSERT INTO dfp_display (ORDER_NAME, LINE_ITEM_NAME, CREATIVE_NAME, LINE_ITEM_TYPE, ADVERTISER_NAME,
                    ORDER_ID, LINE_ITEM_ID, CREATIVE_ID, ADVERTISER_ID, LINE_ITEM_FREQUENCY_CAP, LINE_ITEM_GOAL_QUANTITY,
                    LINE_ITEM_END_DATE_TIME, LINE_ITEM_START_DATE_TIME, LINE_ITEM_COST_PER_UNIT, LINE_ITEM_COMPUTED_STATUS,
                    LINE_ITEM_DISCOUNT, ORDER_SALESPERSON, ORDER_PO_NUMBER, TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS,
                    TOTAL_LINE_ITEM_LEVEL_CLICKS, TOTAL_LINE_ITEM_LEVEL_CTR, TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE,
                    TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS_RATE)
                    VALUES {values};
                '''
        snowflake_hook.run(query)
        logging.info('Batch insert completed successfully')

    except Exception as e:
        logging.error(f'Error inserting data into Snowflake: {e}')
        raise


with (DAG(
        dag_id='ad_manager',
        default_args=default_args,
        schedule_interval=None,
        catchup=False
) as dag):
    create_campaign_report_task = PythonOperator(
        task_id='create_campaign_report',
        python_callable=create_campaign_report,
        provide_context=True,
        # on_failure_callback=send_slack_error_notification
    )

    create_display_report_task = PythonOperator(
        task_id='create_report_display',
        python_callable=create_display_report,
        provide_context=True,
        # on_failure_callback=send_slack_error_notification
    )

    push_to_snowflake_task = PythonOperator(
        task_id='push_to_snowflake',
        python_callable=push_to_snowflake,
        provide_context=True,
        # on_failure_callback=send_slack_error_notification
    )

    create_campaign_report_task >> create_display_report_task