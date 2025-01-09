# region fact_instagram_accounts_daily_data with logging (fetching all facebook pages and their connected instagram account)

import logging

# Set up logging
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Log the start of the script
logging.info("Script started.")

# Set the logging level for the Snowflake connector to WARNING or higher
logging.getLogger("snowflake").setLevel(logging.WARNING)
logging.getLogger("numexpr").setLevel(logging.WARNING)

####imports###
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.models import Variable
    import requests
    import pandas as pd
    # import sys
    from datetime import datetime, timedelta
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.sql import text
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError
    logging.info("All imports were successful.")
except ImportError as e:
    logging.error(f"Failed to import required module: {e}")
    raise

ACCESS_TOKEN = "EAAMocfT7JiABO6p4y3vpcEOiNISSWUokh0RyxO2Ndg0MYWWRUETVK5nsEXXsJiSnlvW17xZCheYcIE2O6TKDDMnwJwnm7tjbue3pUAVCR0FLi4JBarvEXDVImPrgrxYOixzf4eOG0qg6ZCZCB3saT7VK2pkNf3Va9FD1PiXssLyiXLdZClOH0BPGphqsKwnI"
# access token expire on february 15 2025. to extend go here https://developers.facebook.com/tools/debug/accesstoken/
BUSINESS_ACCOUNT_ID = "10154132454588797"  # The Facebook Business Account ID
# PAGE_ID = "100462013796" #mako#       # Facebook Page ID connected to Instagram Business Account
SNOWFLAKE_CONFIG = {
# Snowflake connection parameters
"user" : "OmerY",
"password" : "Oy!273g4",
"account" : "el66449.eu-central-1",
"warehouse" : "COMPUTE_WH",
"database" : "MAKO_DATA_LAKE",
"schema" : "PUBLIC"
}

# Define the date range (has to be in the last 30 days)
SINCE_DATE = (datetime.now() - timedelta(days=30)).date().strftime("%Y-%m-%d")  # Start date (YYYY-MM-DD)
UNTIL_DATE = datetime.now().date().strftime("%Y-%m-%d")  # End date (YYYY-MM-DD)


# Function to convert date to UNIX timestamp
def date_to_unix(date_str):
    return int(datetime.strptime(date_str, "%Y-%m-%d").timestamp())


# Calculate UNIX timestamps for the date range
SINCE_TIMESTAMP = date_to_unix(SINCE_DATE)
UNTIL_TIMESTAMP = date_to_unix(UNTIL_DATE)

# print(SINCE_TIMESTAMP)
# print(SINCE_DATE)
# Initialize a dictionary to store unique pages (key = page_id)
unique_pages = {}


# Function to fetch pages and Instagram accounts
def fetch_pages(endpoint, params, source_name,unique_pages,**kwargs):
    response = requests.get(endpoint, params=params)
    data = response.json()

    if "data" in data:
        for page in data["data"]:
            page_id = page.get("id")  # Unique key for deduplication
            page_name = page.get("name", "Unnamed Page")
            instagram_account = page.get("instagram_business_account", {}).get("id", None)

            # Add to dictionary if not already added
            if page_id not in unique_pages:
                unique_pages[page_id] = {
                    "facebook_page_name": page_name,
                    # "facebook_page_id": page_id,
                    "instagram_account_id": instagram_account,
                    "source": source_name
                }
    else:
        logging.error(f"Error fetching pages from {source_name}: {data}")


# Function to fetch the Instagram account username
def get_instagram_username(instagram_account_id):
    url = f"https://graph.facebook.com/v21.0/{instagram_account_id}"
    params = {
        "fields": "username",
        "access_token": ACCESS_TOKEN
    }
    response = requests.get(url, params=params)
    data = response.json()
    return data.get("username", "Unknown")


# Function to fetch total followers for an Instagram account
def get_total_followers(instagram_account_id):
    url = f"https://graph.facebook.com/v21.0/{instagram_account_id}"
    params = {
        "fields": "followers_count",
        "access_token": ACCESS_TOKEN
    }
    response = requests.get(url, params=params)
    data = response.json()
    return data.get("followers_count", 0)


# Main function to create the fact_instagram_accounts_daily_data table
def create_fact_instagram_accounts_daily_data(**kwargs):
    try:
        logging.info("Script started successfully.")
        # Initialize a dictionary to store unique pages (key = page_id)
        unique_pages = {}
        # Step 1: Fetch Pages from /owned_pages
        logging.info("Fetching Pages from Owned Pages Endpoint")
        owned_pages_url = f"https://graph.facebook.com/v21.0/{BUSINESS_ACCOUNT_ID}/owned_pages"
        params = {
            "fields": "id,name,instagram_business_account",
            "access_token": ACCESS_TOKEN
        }
        fetch_pages(owned_pages_url, params, "owned_pages", unique_pages)

        # Step 2: Fetch Pages from /me/accounts
        logging.info("Fetching Pages from Me Accounts Endpoint")
        accounts_url = "https://graph.facebook.com/v21.0/me/accounts"
        fetch_pages(accounts_url, params, "me/accounts", unique_pages)

        # Step 3: Fetch Pages from /client_pages
        logging.info("Fetching Pages from Client Pages Endpoint")
        client_pages_url = f"https://graph.facebook.com/v21.0/{BUSINESS_ACCOUNT_ID}/client_pages"
        fetch_pages(client_pages_url, params, "client_pages", unique_pages)

        # Step 4: Convert Pages to DataFrame
        df_pages = pd.DataFrame.from_dict(unique_pages, orient="index")
        logging.info("Combined Pages and Instagram Accounts")

        # Step 5: Fetch Reach and Impressions for Instagram Accounts
        metrics = ["reach", "impressions", "follower_count"]  # Desired metrics
        records = []  # To store all metric results

        logging.info("Fetching Reach and Impressions Metrics for Each Instagram Account")

        for _, row in df_pages.iterrows():
            facebook_page_name = row["facebook_page_name"]
            # facebook_page_id = row["facebook_page_id"]
            instagram_account_id = row["instagram_account_id"]

            if instagram_account_id and instagram_account_id != "None":
                instagram_username = get_instagram_username(instagram_account_id)  # Fetch username
                total_followers = get_total_followers(instagram_account_id)  # Fetch total followers
                logging.info(f"Fetching metrics for {instagram_account_id} (Instagram Username: {instagram_username})")

                insights_url = f"https://graph.facebook.com/v21.0/{instagram_account_id}/insights"
                params = {
                    "metric": ",".join(metrics),
                    "period": "day",
                    "since": SINCE_TIMESTAMP,
                    "until": UNTIL_TIMESTAMP,
                    "access_token": ACCESS_TOKEN
                }

                response = requests.get(insights_url, params=params)
                insights_data = response.json()

                # Parse insights data and combine metrics into one row per day
                if "data" in insights_data:
                    # Create a dictionary for metrics by day
                    metrics_by_day = {}

                    for metric_data in insights_data["data"]:
                        metric_name = metric_data["name"]
                        for value in metric_data.get("values", []):
                            date = datetime.strptime(value["end_time"],
                                                     "%Y-%m-%dT%H:%M:%S+0000").date()  # Convert the datetime string to a date (YYYY-MM-DD)
                            if date not in metrics_by_day:
                                metrics_by_day[date] = {
                                    "date": date,
                                    "total_followers": total_followers, }
                            metrics_by_day[date][metric_name] = value["value"]

                    # Add records for each day
                    for date, metrics_data in metrics_by_day.items():
                        record = {
                            # "facebook_page_name": facebook_page_name,
                            # "facebook_page_id": facebook_page_id,
                            # "instagram_account_name": instagram_username,
                            "instagram_account_id": str(instagram_account_id),
                            **metrics_data  # Add metrics (date + all metrics for the day)
                        }
                        records.append(record)
                else:
                    logging.error(f"  Error fetching metrics for {instagram_account_id}: {insights_data}")
            else:
                logging.info(f"Skipping {facebook_page_name} as no Instagram Business Account ID is linked.")

        # Step 5: Convert Metrics to DataFrame
        df_metrics = pd.DataFrame(records)
        df_metrics['last_update'] = datetime.now().date()

        # Save to CSV
        #current_date = datetime.now().strftime("%d_%m_%y")
        #try:
        #    df_metrics.to_csv(
        #        f"C:/Users/omer.yarchi/Desktop/Graph API for Instagram Insights/fact_instagram_accounts_daily_data/fact_instagram_accounts_daily_data_{current_date}.csv",
        #        index=False, encoding='utf-8-sig')
        #    logging.info(f"Data saved to fact_instagram_accounts_daily_data_{current_date}.csv successfully")
        #except Exception as e:
        #    logging.error(f"Error saving csv: {e}")

        # upload data to snowflake table mako_data_lake.public.fact_instagram_accounts_daily_data

        return records
        # Snowflake connection parameters
    except Exception as e:
        logging.error(f"Error: {e}")
        raise

def save_to_snowflake(ti, **kwargs):
    table = "fact_instagram_accounts_daily_data"
    table_staging = "fact_instagram_accounts_daily_data_staging"
    try:
        # Pull `records` from XCom
        records = ti.xcom_pull(task_ids="create_fact_instagram_accounts_daily_data")
        if not records:
            logging.error("No data found in XCom for `records`.")
            raise ValueError("No data found in XCom.")

        # Convert Results to a DataFrame
        df_metrics = pd.DataFrame(records)
        # Debugging logs
        logging.info(f"df_metrics columns: {df_metrics.columns}")
        logging.info(f"df_metrics head: {df_metrics.head()}")

        if "instagram_account_id" not in df_metrics.columns:
            logging.error("The column 'instagram_account_id' is missing in the DataFrame.")
            return

        df_metrics['last_update'] = datetime.now().date()
    except Exception as e:
        logging.error(f"Error: {e}")

    # Connect to Snowflake using SQLAlchemy
    engine = create_engine(
        f"snowflake://{SNOWFLAKE_CONFIG['user']}:{SNOWFLAKE_CONFIG['password']}@{SNOWFLAKE_CONFIG['account']}/"
        f"{SNOWFLAKE_CONFIG['database']}/{SNOWFLAKE_CONFIG['schema']}?warehouse={SNOWFLAKE_CONFIG['warehouse']}"
    )

    # Upload data to fact_instagram_accounts_daily_data_staging
    try:
        # If the table doesn't exist, create it; otherwise, append data
        df_metrics.to_sql(
            f"{table_staging}",
            con=engine,
            index=False,  # Do not write the DataFrame index as a separate column
            if_exists="replace",  # Options: 'fail', 'replace', 'append'
            method="multi",  # Optimized bulk insert
        )
        logging.info(f"Data uploaded successfully to {table_staging}")
    except Exception as e:
        logging.error(f"Error uploading data: {e}")

    # Query to count rows in the staging table
    try:
        Session = sessionmaker(bind=engine)
        session = Session()
        # rows in the staging table
        count_new_rows = text(f"""select count(*) as row_count from {table_staging}""")
        result = session.execute(count_new_rows)
        row_count = result.scalar()
        logging.info(f"Number of rows inserted to {table_staging}: {row_count}")
        # rows in dim_instagram_accounts before upsert
        count_new_rows_dim = text(f"""select count(*) as row_count from {table}""")
        result_dim = session.execute(count_new_rows_dim)
        row_count_dim = result_dim.scalar()
        logging.info(f"Number of rows exist in {table} (before upsert): {row_count_dim}")
    except Exception as e:
        logging.error(f"Error counting rows in {table_staging} or {table}: {e}")
        session.rollback()
    finally:
        session.close()

    # Perform the UPSERT operation using a Snowflake SQL query
    Session = sessionmaker(bind=engine)
    session = Session()

    upsert_query = text(f"""
    MERGE INTO {table} AS target
    USING {table_staging} AS source
    ON target.instagram_account_id = source.instagram_account_id and target.date = source.date --table keys
    WHEN MATCHED THEN
        UPDATE SET 
            reach = source.reach,
            impressions = source.impressions,
            follower_count = source.follower_count,
            last_update = source.last_update
    WHEN NOT MATCHED THEN
        INSERT (instagram_account_id, date, total_followers, reach, impressions, follower_count, last_update)
        VALUES (source.instagram_account_id, source.date, source.total_followers, source.reach, source.impressions, source.follower_count, source.last_update);
    """)

    try:
        session.execute(upsert_query)
        session.commit()
        logging.info("Upsert operation completed successfully.")
    except Exception as e:
        logging.error(f"Error during upsert: {e}")
        session.rollback()
    finally:
        session.close()

    # Query to count rows in the fact after upsert
    try:
        Session = sessionmaker(bind=engine)
        session = Session()

        count_rows_after_upsert = text(f"""select count(*) as row_count from {table}""")
        result_after_upsert = session.execute(count_rows_after_upsert)
        row_count_after_upsert = result_after_upsert.scalar()
        logging.info(f"Number of rows in {table} (after upsert): {row_count_after_upsert}")
    except Exception as e:
        logging.error(f"Error counting rows in {table} after upsert: {e}")
        session.rollback()
    finally:
        session.close()

    try:
        # Update table_last_update to the current date
        update_table_last_update_query = text(f"""
        UPDATE {table}
        SET table_last_update = :current_date
        """)
        session.execute(update_table_last_update_query, {"current_date": datetime.now().date()})
        session.commit()
        logging.info("table_last_update column updated successfully.")
    except Exception as e:
        logging.error(f"Error updating table_last_update: {e}")
        session.rollback()
    finally:
        session.close()

    # Drop the staging table
    try:

        cleanup_query = text(f"DROP TABLE IF EXISTS {SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}.{table_staging}")
        session.execute(cleanup_query)
        logging.info(f"Table {table_staging} dropped successfully.")
        session.commit()
    except Exception as e:
        logging.error(f"Error dropping staging table: {e}")
        session.rollback()
    finally:
        session.close()


# endregion
#######################

default_args = {
    'owner': 'airflow_omer',
    #'start_date':datetime(2024, 1, 7),
    'email': ['omer.yarchi@keshet-d.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=3)
}

with DAG(
    dag_id="fact_instagram_accounts_daily_data",
    default_args=default_args,
    description="Fetch Instagram accounts daily metrics data and upload to Snowflake",
    schedule_interval="0 15 * * *",  # Run daily at 17:00
    start_date=datetime(2025, 1, 7),
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id="create_fact_instagram_accounts_daily_data",
        python_callable=create_fact_instagram_accounts_daily_data,
    )

    save_task = PythonOperator(
        task_id="save_to_snowflake",
        python_callable=save_to_snowflake,
    )

    fetch_task >> save_task













