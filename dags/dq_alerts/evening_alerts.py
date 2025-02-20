from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import timedelta
import pendulum
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import logging
from dq_alerts.dq_alerts_utils import *
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2025, 1, 1, tz='Asia/Jerusalem'),
    'email': ['liron.akuny@keshet-d.co.il'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=3),
    'provide_context': True
}

SNOWFLAKE_CONN_ID = 'mako_snowflake'
snowflake_sql_dir = os.path.join(os.path.dirname(__file__), 'snowflake_sql_queries')  # Directory for SQL files

def validate_snowflake_queries(**kwargs):
    context = kwargs
    log.info("Starting validation of Snowflake queries.")

    queries = {
        "Load_failed": read_file(snowflake_sql_dir,'load_failed.sql'),
        "Percentage_rows_failed": read_file(snowflake_sql_dir,'percentage_rows_failed.sql')
    }
    log.info(f"Loaded queries: {list(queries.keys())}")

    execute_snowflake_queries(queries, SNOWFLAKE_CONN_ID, context)


with ((DAG(
        'evening_alerts',
        default_args = default_args,
        description = "Run multiple Snowflake data test queries using PythonOperator",
        schedule_interval = None,
        catchup = False
)) as dag):
    snowflake_test = PythonOperator(
        task_id = 'validate_snowflake_queries',
        python_callable = validate_snowflake_queries,
        on_failure_callback = send_slack_error_notification
    )

snowflake_test