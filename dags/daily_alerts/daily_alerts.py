from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from datetime import timedelta
import pendulum
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import logging
import os
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import requests
import json
import pandas as pd
from daily_alerts.domo_dataset_validations.dataset_validations import dataset_list

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
SNOWFLAKE_SQL_DIR = os.path.join(os.path.dirname(__file__), 'snowflake_sql_queries')  # Directory for SQL files

domo_user = Variable.get('domo_user')
domo_password = Variable.get('domo_password')
domoKey =  Variable.get('domoKey')
domoSecret = Variable.get('domoSecret')

auth_url_keshet_tv = "https://keshet-tv.domo.com/api/content/v2/authentication"
search_url = "https://keshet-tv.domo.com/api/data/ui/v3/datasources/search"
auto_url_domo = "https://api.domo.com/oauth/token?grant_type=client_credentials"
tag_value = "Daily Monitoring"

def send_slack_error_notification(context, message = 'None'):
    """
     Sends an error notification to Slack.
     Parameters:
     - context (dict): Airflow context dictionary for automatic error reporting.
     - message (str, optional): Custom error message to send.
     """

    slack_token = Variable.get('slack_token')
    slack_channel = 'C088Z2ELD6U'

    if message is 'None':
        message = f"An error occurred in the Airflow DAG '{context['dag'].dag_id}' on {context['execution_date']}.\nError Message: {context['exception']}"

    try:
        client = WebClient(token=slack_token)
        response = client.chat_postMessage(channel=slack_channel, text=message)
        assert response['message']['text'] == message

        log.info(f"Slack message sent successfully: {response['message']['text']}")
    except SlackApiError as e:
        log.error(f"Error sending Slack message: {e.response['error']}")

def make_request(method, url, headers=None, payload=None, auth=None):
    """
    A generic function to make GET or POST HTTP requests.

    Parameters:
        method (str): HTTP method ('GET' or 'POST').
        url (str): URL for the request.
        headers (dict, optional): HTTP headers for the request.
        payload (dict or str, optional): Data to send in the request body (for POST).
        auth (tuple, optional): Basic authentication credentials (domo_user, domo_password).

    Returns:
        response: The response object from the request.
    """
    try:
        # Choose the request method dynamically
        if method.upper() == "POST":
            response = requests.post(url, headers=headers, data=payload, auth=auth)
        elif method.upper() == "GET":
            response = requests.get(url, headers=headers, params=payload, auth=auth)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        # Log response details
        log.info(f"Status Code: {response.status_code}")
        # log.info(f"Response Body: {response.text}")

        # Return the response object
        return response
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def get_dataset(url, token):
    # Define the variable that determines which query to use
    if url == "https://api.domo.com/v1/datasets/query/execute/07befa93-15c4-4cef-9a05-5cd701ef404c":
        sql_query = "select * from table where server_ts::DATE >= CURDATE()-1 LIMIT 10"
    else:
        sql_query = "SELECT * FROM table WHERE date = CURDATE()-1 LIMIT 10"

    payload = json.dumps({
        "sql": sql_query
    })

    headers = {
        'Authorization': 'Bearer ' + token,
        'Content-Type': 'application/json'
    }
    response = make_request(
        method="POST",
        url=url,
        headers=headers,
        payload=payload
    )
    return response

def validate_dataset(url, token, min_rows=1, conditions=None):
    """
    Validate a dataset against given conditions.

    Parameters:
        url (str): Dataset URL.
        token (str): API token.
        min_rows (int): Minimum number of rows required.
        conditions (dict): Column-specific validation conditions.
    """
    datasets = get_dataset(url, token)
    datasets = datasets.json()  # Convert the Response object to a json
    # Extract the 'columns' field
    columns = datasets['columns']
    table = datasets.get('rows', [])
    # Check row count
    if not datasets or len(datasets) < min_rows:
        log.error(f"Dataset from {url} has insufficient rows. Found {len(datasets)}, required {min_rows}.")
        return False

    # Check conditions
    if conditions:
        df = pd.DataFrame(table, columns=columns)
        for col, condition in conditions.items():
            try:
                if not condition(df[col]):
                    log.error(f"Validation failed for column '{col}' in dataset from {url}.")
                    return False
            except Exception as e:
                log.error(f"Error validating column '{col}' in dataset from {url}: {e}")
                return False
    return True

def validate_snowflake_queries(**kwargs):
    context = kwargs
    # Function to read a SQL file
    def read_sql_file(file_name):
        with open(os.path.join(SNOWFLAKE_SQL_DIR, file_name), 'r') as file:
            return file.read()
    log.info("Starting validation of Snowflake queries.")

    queries = {
        "last_combined_events_update": read_sql_file('last_combined_events_update.sql'),
        "last_dataenrichment_update": read_sql_file('last_dataenrichment_update.sql'),
        "last_epg_update": read_sql_file('last_epg_update.sql'),
        "events_data_comparison": read_sql_file('events_data_comparison.sql'),
        "device_data_users": read_sql_file('device_data_users.sql'),
        "page_view_by_hour": read_sql_file('page_view_by_hour.sql')
    }
    log.debug(f"Loaded queries: {list(queries.keys())}")

    # Connect to Snowflake using Airflow Hook
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    error_message = []
    try:
        for query_name, query in queries.items():
            log.info(f"Executing query: {query_name}")
            with conn.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()[0]
                log.info(f"Query '{query_name}' returned result: {result}")
                if result != "SUCCESS":
                    error_message.append(f"Query '{query_name}' failed!")
    except Exception as e:
        log.error(f"Error during query execution: {e}", exc_info=True)
        # raise
    finally:
        conn.close()
    # Raise an error if any query failed
    if error_message:
        # error_message = "\n".join(error_message)
        log.error(f"Validation failed with errors: {error_message}")
        # raise ValueError(error_message)
        message = f"Validation failed with errors:\n {error_message}"
        send_slack_error_notification(context,message)
    else:
        log.info("All queries executed successfully!")

def validate_domo_dataset_running(timezone_name="Asia/Jerusalem", **kwargs ):
    context = kwargs
# Step 1: Authenticate and get a session token to keshet-tv
    log.info("Authenticating to keshet-tv.domo.com")
    auth_payload = json.dumps({
                "method": "password",
                "emailAddress": domo_user,
                "password": domo_password
            })
    headers = {'Content-Type': 'application/json'}

    response = make_request(
        method="POST",
        url=auth_url_keshet_tv,
        headers=headers,
        payload=auth_payload
    )
    token = response.json().get('sessionToken')
    log.debug(f"Session Token: {token}")

    # Step 2: Search for datasets with the specified tag
    log.info(f"Searching for datasets with tag: {tag_value}")
    search_payload = json.dumps({
        "entities": ["DATASET"],
        "filters": [{"field": "tag_facet", "filterType": "term", "value": tag_value}],
        "combineResults": True,
        "query": "*",
        "count": 100,
        "offset": 0,
        "sort": {
            "isRelevance": False,
            "fieldSorts": [{"field": "card_count", "sortOrder": "DESC"}]
        }
    })
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'x-domo-authentication': token
    }
    response = make_request(
        method="POST",
        url=search_url,
        headers=headers,
        payload=search_payload
    )
    datasets = response.json().get('dataSources', [])
    log.info(f"Found {len(datasets)} datasets.")

    # Step 3: Validate datasets' last updated date
    today = datetime.now(ZoneInfo(timezone_name)).date()
    log.debug(f"Today's date in {timezone_name}: {today}")

    error_message = []
    for dataset in datasets:
        name = dataset.get('name')
        last_updated = dataset.get('lastUpdated')
        last_updated_date = datetime.fromtimestamp(last_updated / 1000, tz=ZoneInfo(timezone_name)).date()

        if last_updated_date != today:
            error_message.append(
                f"Dataset '{name}' was last updated on {last_updated_date}, not today."
            )

    # Log and return results
    if error_message:
        log.info("Some datasets were not updated today.")
        for error in error_message:
            log.info(error)
        message = f"Some datasets were not updated today: {error_message}"
        send_slack_error_notification(context, message)
    else:
        log.info("All datasets were updated today.")

    return error_message

def validate_domo_dashboards(**kwargs):
    context = kwargs
    # Authenticating
    log.info("Authenticating to api.domo.com")
    response = make_request(
        method="GET",
        url=auto_url_domo,
        auth=(domoKey, domoSecret)
    )
    token = response.json().get('access_token')
    log.info(f"Session Token: {token}")
    error_message = []
    for dataset in dataset_list :
        url = dataset.get("url")
        min_rows = dataset.get("min_rows", 1)
        conditions = dataset.get("conditions", {})

        # Perform validation
        log.info(f"Validating dataset: {url}")
        result = validate_dataset(url, token, min_rows=min_rows, conditions=conditions)

        if not result:
            log.error(f"Validation failed for dataset: {url}")
            error_message.append(
                f"Validation failed for dataset: {url}")
        else:
            log.info(f"Validation succeeded for dataset: {url}")
        log.info("------------------------------------")
    if error_message:
        message = f"Validation failed for dataset:\n {error_message}"
        send_slack_error_notification(context, message)

# Define the DAG
with ((DAG(
        'daily_alerts',
        default_args = default_args,
        description = "Run multiple Snowflake data test queries using PythonOperator",
        schedule_interval = '10 7 * * *',
        catchup = False
)) as dag):
    snowflake_test = PythonOperator(
        task_id = 'validate_snowflake_queries',
        python_callable = validate_snowflake_queries,
        on_failure_callback = send_slack_error_notification
    )
    domo_dataset_running_test = PythonOperator(
        task_id = 'validate_domo_dataset_running',
        python_callable = validate_domo_dataset_running,
        on_failure_callback = send_slack_error_notification
    )
    domo_dashboard = PythonOperator(
        task_id = 'validate_domo_dashboards',
        python_callable = validate_domo_dashboards,
         on_failure_callback = send_slack_error_notification
    )

    snowflake_test >> domo_dataset_running_test >> domo_dashboard
