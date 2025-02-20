from airflow.models import Variable
import os
import logging
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
log = logging.getLogger(__name__)


def send_slack_error_notification(context, message="None"):
    """
    Sends an error notification to Slack.

    :param context: Airflow context dictionary for automatic error reporting.
    :param message: Custom error message to send (default: extracted from context).
    """

    slack_token = Variable.get("slack_token")
    slack_channel = Variable.get("alert_slack_channel")
    if message == "None":
        message = (
            f"An error occurred in the Airflow DAG '{context['dag'].dag_id}' "
            f"on {context['execution_date']}.\nError Message: {context.get('exception', 'No error message')}"
        )

    try:
        client = WebClient(token=slack_token)
        response = client.chat_postMessage(channel=slack_channel, text=message)
        log.info(f"Slack message sent successfully: {response['message']['text']}")
    except SlackApiError as e:
        log.error(f"Error sending Slack message: {e.response['error']}")

def read_file(dir, file_name):
    with open(os.path.join(dir, file_name), 'r') as file:
        return file.read()


def execute_snowflake_queries(queries, snowflake_conn_id, context):
    """
    Executes multiple queries in Snowflake and logs results.
    Sends a Slack notification if any query fails.

    :param queries: Dictionary of query names and SQL queries.
    :param snowflake_conn_id: Airflow Snowflake connection ID.
    :param context: Airflow context for notifications.
    :return: True if all queries succeed, False otherwise.
    """

    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
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
                    error_message.append(f"Query '{query_name}' failed - {result}%")
    except Exception as e:
        log.error(f"Error during query execution: {e}", exc_info=True)
        error_message.append(str(e))
    finally:
        conn.close()

    if error_message:
        message = f"Validation failed with errors:\n {error_message}"
        log.error(message)
        send_slack_error_notification(context, message)
        return False

    log.info("All queries executed successfully!")
    return True