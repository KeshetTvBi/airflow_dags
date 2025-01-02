from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from datetime import timedelta
# from utilities.graph_utils import graph_client
import pendulum
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
# from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import logging
# import io
# from pydomo import Domo
# from pydomo.datasets import UpdateMethod
# import csv
import os
# from slack_sdk import WebClient
# from slack_sdk.errors import SlackApiError
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
import requests
import json
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2023, 9, 4, tz='Asia/Jerusalem'),
    # 'email': ['liron.akuny@keshet-d.co.il'],
    #'email_on_failure': True,
    #'email_on_retry': False,
    # 'retries': 0,
    # 'retry_delay': timedelta(minutes=3),
    # 'provide_context': True
}

SNOWFLAKE_CONN_ID = 'mako_snowflake'
SNOWFLAKE_SQL_DIR = os.path.join(os.path.dirname(__file__), 'snowflake_sql_queries')  # Directory for SQL files
DOMO_SQL_DIR = os.path.join(os.path.dirname(__file__), 'domo_sql_queries')
auth_url_keshet_tv = "https://keshet-tv.domo.com/api/content/v2/authentication"
search_url = "https://keshet-tv.domo.com/api/data/ui/v3/datasources/search"
auto_url_domo = "https://api.domo.com/oauth/token?grant_type=client_credentials"

username = Variable.get('domo_user')
password = Variable.get('domo_password')
api_domo_username =  Variable.get('api_domo_username')
api_domo_password = Variable.get('api_domo_password')

tag_value = "Daily Monitoring"

# Python function to execute queries and check results
def validate_snowflake_queries():
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
    error_messages = []
    try:
        for query_name, query in queries.items():
            log.info(f"Executing query: {query_name}")
            with conn.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()[0]
                log.info(f"Query '{query_name}' returned result: {result}")
                if result != "SUCCESS":
                    error_messages.append(f"Query '{query_name}' failed!")
    except Exception as e:
        log.error(f"Error during query execution: {e}", exc_info=True)
        raise
    finally:
        conn.close()
    # Raise an error if any query failed
    if error_messages:
        error_message = "\n".join(error_messages)
        log.error(f"Validation failed with errors: {error_message}")
        raise ValueError(error_message)
    else:
        log.info("All queries executed successfully!")


def make_request(method, url, headers=None, payload=None, auth=None):
    """
    A generic function to make GET or POST HTTP requests.

    Parameters:
        method (str): HTTP method ('GET' or 'POST').
        url (str): URL for the request.
        headers (dict, optional): HTTP headers for the request.
        payload (dict or str, optional): Data to send in the request body (for POST).
        auth (tuple, optional): Basic authentication credentials (username, password).

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
        print(f"Status Code: {response.status_code}")
        print(f"Response Body: {response.text}")

        # Return the response object
        return response
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def validate_domo_dataset_running(timezone_name="Asia/Jerusalem"):
    # Step 1: Authenticate and get a session token to keshet-tv
    log.info("Authenticating to keshet-tv.domo.com")
    auth_payload = json.dumps({
                "method": "password",
                "emailAddress": username,
                "password": password
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

    error_messages = []
    for dataset in datasets:
        name = dataset.get('name')
        last_updated = dataset.get('lastUpdated')
        last_updated_date = datetime.fromtimestamp(last_updated / 1000, tz=ZoneInfo(timezone_name)).date()

        if last_updated_date != today:
            error_messages.append(
                f"Dataset '{name}' was last updated on {last_updated_date}, not today."
            )

    # Log and return results
    if error_messages:
        log.warning("Some datasets were not updated today.")
        for error in error_messages:
            log.warning(error)
    else:
        log.info("All datasets were updated today.")

    return error_messages

def get_dataset(url, token):
    payload = json.dumps({

        "sql": "SELECT * FROM table WHERE date = CURDATE()-1"
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
    datasets = response.json().get('rows', [])
    return datasets


import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.ERROR)
log = logging.getLogger(__name__)


def validate_dataset(url, token, columns, type_col=None, expected_unique_types=None, conditions=None):
    """
    Validate a dataset retrieved from a given URL.

    Parameters:
        url (str): The API URL to fetch the dataset.
        token (str): The API token for authentication.
        columns (list): List of column names expected in the dataset.
        type_col (str): Name of the column to check unique type count (optional).
        expected_unique_types (int): Expected count of unique values in `type_col` (optional).
        conditions (dict): A dictionary where keys are column names and values are callables
                           that take a column and return a boolean (e.g., lambda x: (x > 0).all()).
    """
    # Fetch the dataset
    datasets = get_dataset(url, token)

    # Convert to a DataFrame
    df = pd.DataFrame(datasets, columns=columns)

    # Check unique types if applicable
    if type_col and expected_unique_types is not None:
        unique_types = df[type_col].nunique()
        if unique_types != expected_unique_types:
            log.error(
                f"Missing data, not all expected types exist in {url}. Found {unique_types}, expected {expected_unique_types}.")

    # Apply conditions
    if conditions:
        for col, condition in conditions.items():
            if not condition(df[col]):
                log.error(f"Validation failed for column '{col}' in dataset from {url}.")

def validate_domo_dashboards():
    # Authenticating
    log.info("Authenticating to api.domo.com")
    response = make_request(
        method="GET",
        url=auto_url_domo,
        auth=(api_domo_username, api_domo_password)
    )
    token = response.json().get('access_token')
    log.info(f"Session Token: {token}")

    # ------- View of Daily_revenue_no_grouping -------
    url = "https://api.domo.com/v1/datasets/query/execute/31131a4d-3c9a-4e4c-a133-3170b4160452"
    columns = ["date", "weekday", "type", "minimum_goal", "sum_revenue"]
    validate_dataset(
        url=url,
        token=token,
        columns=columns,
        type_col="type",
        expected_unique_types=6,
        conditions={
            "minimum_goal": lambda x: (x > 0).all(),
            "sum_revenue": lambda x: (x > 0).all()
        }
    )
    # ------- s_yesterday_mako_new_2 -------
    url = "https://api.domo.com/v1/datasets/query/execute/3c971771-55e4-4de6-ace4-e1aaf3e405b5"
    columns = ["UNIQUES", "PV", "SITE", "DATE", "DDATE", "DAYNUM", "AVGPV", "AVGUSER"]
    validate_dataset(
        url=url,
        token=token,
        columns=columns,
        type_col="SITE",  # Adjust as per your actual unique type column
        expected_unique_types=5,
        conditions={
            "UNIQUES": lambda x: (x > 0).all(),
            "PV": lambda x: (x > 0).all()
        }
    )
    # ------- m_channels_daily_model -------

#todo:    m_channels_daily_model
    m_items_pageview_daily
    m_channel_daily_goals
    m_items_pageview_daily
    m_12Plus_players_new
    mako_n12_player_events

    # #--------------------------------------------------
    # # ------- View of Daily_revenue_no_grouping -------
    # # -------------------------------------------------
    # url = "https://api.domo.com/v1/datasets/query/execute/31131a4d-3c9a-4e4c-a133-3170b4160452"
    # datasets = get_dataset(url, token)
    # # Convert to a DataFrame
    # columns = ["date", "weekday", "type", "minimum_goal", "sum_revenue"]
    # df = pd.DataFrame(datasets, columns=columns)
    # # Check if there are exactly 6 unique types
    # unique_types = df['type'].nunique()
    # types_check = unique_types == 6
    # # Check if all minimum_goal and sum_revenue are greater than 0
    # min_goal_check = (df['minimum_goal'] > 0).all()
    # sum_revenue_check = (df['sum_revenue'] > 0).all()
    # # Print the results
    # if not types_check:
    #     log.error(f"Missing data, not all types exist")
    # if not min_goal_check or not sum_revenue_check:
    #     log.error("There are values equal to or less than 0 in Daily_revenue_no_grouping view dataset")
    # #---------------------------------------
    # # ------- s_yesterday_mako_new_2 -------
    # # --------------------------------------
    # url = "https://api.domo.com/v1/datasets/query/execute/3c971771-55e4-4de6-ace4-e1aaf3e405b5"
    # datasets = get_dataset(url, token)
    # # Convert to a DataFrame
    # columns = ["UNIQUES", "PV", "SITE", "DATE", "DDATE", "DAYNUM", "AVGPV", "AVGUSER"]
    # df = pd.DataFrame(datasets, columns=columns)
    # # Check if there are exactly 5 unique types
    # unique_types = df['type'].nunique()
    # types_check = unique_types == 5
    # # Check if all uniques and pv are greater than 0
    # min_goal_check = (df['minimum_goal'] > 0).all()
    # sum_revenue_check = (df['sum_revenue'] > 0).all()
    # # Print the results
    # if not types_check:
    #     log.error(f"Missing data, not all types exist")
    # if not min_goal_check or not sum_revenue_check:
    #     log.error("There are values equal to or less than 0 in Daily_revenue_no_grouping view dataset")
    # # ---------------------------------------
    # # ------- v_comparing_pv_and_play -------
    # # ---------------------------------------
    # # Convert to a DataFrame
    # url = "https://api.domo.com/v1/datasets/query/execute/a1b1fb65-a836-4bec-ba03-b33995859401"
    # columns = ["d_dat", "all_pv", "mako_pv", "12plus_pv", "n12_pv", 'all_plays', 'mako_plays', '12plus_plays', 'n12_plays']
    # df = pd.DataFrame(datasets, columns=columns)
    # # Exclude d_dat and check all other columns
    # columns_to_check = df.columns.difference(["d_dat"])
    # # Check if all values in the selected columns are greater than 0
    # columns_valid = (df[columns_to_check] > 0).all().all()
    # if not columns_valid:
    #     log.error(f"There are values equal to or less than 0 in v_comparing_pv_and_play dataset")
    # # ------------------------------------------------
    # # ------- v_comparing_daily_uniques_pv_new -------
    # # ------------------------------------------------
    # # Convert to a DataFrame
    # url = "https://api.domo.com/v1/datasets/query/execute/36c758ca-35fe-4b57-9b02-3cd93f64a281"
    # columns = ["d_dat", "all_uniques", "12plus_uniques", "n12_uniques", "mako_uniques"]
    # df = pd.DataFrame(datasets, columns=columns)
    # # Exclude d_dat and check all other columns
    # columns_to_check = df.columns.difference(["d_dat"])
    # # Check if all values in the selected columns are greater than 0
    # columns_valid = (df[columns_to_check] > 0).all().all()
    # if not columns_valid:
    #     log.error(f"There are values equal to or less than 0 un v_comparing_daily_uniques_pv_new dataset")
    # # --------------------------------------------------
    # # ------- v_comparing_monthly_uniques_pv_new -------
    # # --------------------------------------------------
    # # Convert to a DataFrame
    # url = "https://api.domo.com/v1/datasets/query/execute/5d6bcae5-2fc7-4e13-bd5f-ac2283d8f9f7"
    # columns = ["yy", "mm", "all_uniques", "12plus_uniques", "n12_uniques", "mako_uniques", "mm_and_yy"]
    # df = pd.DataFrame(datasets, columns=columns)
    # # Exclude d_dat and check all other columns
    # columns_to_check = df.columns.difference(["yy", 'mm'])
    # # Check if all values in the selected columns are greater than 0
    # columns_valid = (df[columns_to_check] > 0).all().all()
    # if not columns_valid:
    #     log.error(f"There are values equal to or less than 0 un v_comparing_pv_and_play dataset")
    # # ----------------------------------------------
    # # ------- v_comparing_uniques_play_new_2 -------
    # # ----------------------------------------------
    # # Convert to a DataFrame
    # url = "https://api.domo.com/v1/datasets/query/execute/ed91f526-5a72-4772-9da4-ed9e8618fc29"
    # columns = ["yy","all_uniques", "12plus_uniques", "n12_uniques", "mako_uniques", "partition"]
    # df = pd.DataFrame(datasets, columns=columns)


    # df["date"] = pd.to_datetime(df["date"])
    # filtered_types = ["Video", "Direct Display", "Programmatic Display"]
    # filtered_type_df = df[df["type"].isin(filtered_types)]
    # # Rename the types
    # type_mapping = {"Video": "video_ws", "Direct Display": "direct_ws", "Programmatic Display": "Programmatic_ws"}
    # filtered_type_df["type"] = filtered_type_df["type"].replace(type_mapping)
    # filtered_type_df["date"] = filtered_type_df["date"].dt.date
    # --------------------------------------------------
    # # Filter for yesterday's rows
    # yesterday = datetime.now() - timedelta(days=1)
    # yesterday = yesterday.date()
    # last_year = datetime.now() - timedelta(days=365)
    # last_year = last_year.date()
    # yesterday_df = filtered_type_df[filtered_type_df["date"] == yesterday]
    # yesterday_result = yesterday_df.groupby("type")[["minimum_goal", "sum_revenue"]].sum().reset_index()
    # yesterday_percentage = ((yesterday_result["sum_revenue"]/yesterday_result["minimum_goal"] - 1) * 100)
    # total_result = yesterday_result[["minimum_goal", "sum_revenue"]].sum()
    # yesterday_revenue_sum = filtered_type_df.loc[filtered_type_df["date"] == yesterday, "sum_revenue"].sum()
    # last_year_revenue_sum = filtered_type_df.loc[filtered_type_df["date"] == last_year, "sum_revenue"].sum()
    #
    # log.info(f"yesterday group by type:\n {yesterday_result}")
    # log.info(f"yesterday percentage sum_revenue/minimum_goal:\n {yesterday_percentage}")
    # log.info(f"yesterday sum:\n {total_result}")
    # log.info(f"Revenue for yesterday:\n {yesterday_revenue_sum}")
    # log.info(f"Revenue for 365 days ago:\n {last_year_revenue_sum}")
    #
    # # Filter rows between the start of the month and today
    # today = datetime.now().date()
    # start_of_month = datetime.now().replace(day=1).date()
    # filtered_month_df = filtered_type_df[(filtered_type_df["date"] >= start_of_month) & (filtered_type_df["date"] < today)]
    # monthly_result = filtered_month_df.groupby("type")[["minimum_goal", "sum_revenue"]].sum().reset_index()
    # monthly_percentage = ((monthly_result["sum_revenue"]/ monthly_result["minimum_goal"] - 1) * 100)
    # total_result = filtered_month_df[["minimum_goal", "sum_revenue"]].sum()
    # log.info(f"monthly group by type:\n {monthly_result}")
    # log.info(f"monthly percentage sum_revenue/minimum_goal:\n {monthly_percentage}")
    # log.info(f"monthly sum:\n {total_result}")
    #
    # # Filter rows between the start of the year and today
    # current_year = datetime.now().year
    # filtered_year_df  = filtered_type_df[(filtered_type_df["date"] == current_year) &(filtered_type_df["date"] != today)]
    # annual_result = filtered_year_df.groupby("type")[["minimum_goal", "sum_revenue"]].sum().reset_index()
    # annual_percentage = ((annual_result["sum_revenue"]/annual_result["minimum_goal"] - 1 ) * 100)
    # total_result = annual_result[["minimum_goal", "sum_revenue"]].sum()
    # log.info(f"annual group by type:\n {annual_result}")
    # log.info(f"annual percentage sum_revenue/minimum_goal:\n {annual_percentage}")
    # log.info(f"annual sum:\n {total_result}")

# def send_slack_error_notification(context):
#     slack_token = '****'
#     slack_channel = 'C05UNMWHX2R'


# Define the DAG
with ((DAG(
        'daily_alerts',
        default_args=default_args,
        description= "Run multiple Snowflake data test queries using PythonOperator",
        schedule_interval=None,
        catchup=False
)) as dag):
    # snowflake_test = PythonOperator(
    #     task_id='validate_snowflake_queries',
    #     python_callable=validate_snowflake_queries
    # )
    # domo_dataset_running_test = PythonOperator(
    #     task_id='validate_domo_dataset_running',
    #     python_callable=validate_domo_dataset_running
    # )
    domo_dashboard = PythonOperator(
        task_id='validate_domo_dashboards',
        python_callable=validate_domo_dashboards
    )

    # snowflake_test >> domo_dataset_running_test
