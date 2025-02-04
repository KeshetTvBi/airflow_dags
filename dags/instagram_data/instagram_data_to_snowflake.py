
#https://developers.facebook.com/docs/instagram-platform/reference/instagram-media/insights
#https://developers.facebook.com/docs/instagram-platform/reference/instagram-media/insights

import logging

# Set up logging
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Set the logging level for the Snowflake connector to WARNING or higher
logging.getLogger("snowflake").setLevel(logging.WARNING)
logging.getLogger("numexpr").setLevel(logging.WARNING)

# region imports
####imports###
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.models import Variable
    from airflow.hooks.base import BaseHook
    import requests
    import pandas as pd
    import asyncio
    import aiohttp
    # import sys
    from datetime import datetime, timedelta
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.sql import text
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError
    import json
    logging.info("All imports were successful.")
except ImportError as e:
    logging.error(f"Failed to import required module: {e}")
    raise
# endregion


# region Graph API and Snowflake settings
ACCESS_TOKEN = "EAAMocfT7JiABO6p4y3vpcEOiNISSWUokh0RyxO2Ndg0MYWWRUETVK5nsEXXsJiSnlvW17xZCheYcIE2O6TKDDMnwJwnm7tjbue3pUAVCR0FLi4JBarvEXDVImPrgrxYOixzf4eOG0qg6ZCZCB3saT7VK2pkNf3Va9FD1PiXssLyiXLdZClOH0BPGphqsKwnI"
# access token expire on february 15 2025. to extend go here https://developers.facebook.com/tools/debug/accesstoken/
BUSINESS_ACCOUNT_ID = "10154132454588797"  # The Facebook Business Account ID
# PAGE_ID = "100462013796" #mako#       # Facebook Page ID connected to Instagram Business Account

airflow_conn_id = "mako_snowflake"
snowflake_conn = BaseHook.get_connection(airflow_conn_id)
# Parse the "extra" JSON field from the connection
extra = json.loads(snowflake_conn.extra)
# Extract credentials
SNOWFLAKE_CONFIG = {
    "user": snowflake_conn.login,
    "password": snowflake_conn.password,
    "account": f"{extra['account']}.{extra['region']}",
    "warehouse": "COMPUTE_WH", #snowflake_conn.extra_dejson.get("warehouse"),
    "database": "MAKO_DATA_LAKE", #snowflake_conn.schema,
    "schema": snowflake_conn.schema,
}

# endregion

#region slack notifications
def send_slack_error_notification(context):
    slack_token = Variable.get("slack_token")
    slack_channel = 'C0897560LDC'

    message = f"<@U07N2CGU56K> An error occurred in the Airflow DAG'{context['dag'].dag_id}' on {context['execution_date']}.\nError Message: {context['exception']}"

    try:
        client = WebClient(token=slack_token)
        response = client.chat_postMessage(channel=slack_channel, text=message)
        assert response['message']['text'] == message
    except SlackApiError as e:
        log.info(f"Error sending Slack message: {e.response['error']}")

def send_slack_success_notification(context):
    slack_token = Variable.get("slack_token")
    slack_channel = 'C0897560LDC'

    message = f"<@U07N2CGU56K> Successful run Airflow DAG '{context['dag'].dag_id}'"

    try:
        client = WebClient(token=slack_token)
        response = client.chat_postMessage(channel=slack_channel, text=message)
        assert response['message']['text'] == message
    except SlackApiError as e:
        log.info(f"Error sending Slack message: {e.response['error']}")
#endregion

# Log the start of the script
logging.info("Script started.")

# region dim_instagram_accounts (fetching all facebook pages and their connected instagram account)
# Initialize a dictionary to store unique pages (key = page_id)
unique_pages_dim_accounts = {}

# Function to fetch Instagram account username
def get_instagram_username(instagram_account_id):
    url = f"https://graph.facebook.com/v21.0/{instagram_account_id}"
    params = {
        "fields": "username",
        "access_token": ACCESS_TOKEN
    }
    response = requests.get(url, params=params)
    data = response.json()
    return data.get("username", "Unknown")


# Function to fetch pages and Instagram accounts
def fetch_pages_dim_accounts(endpoint, params, source_name, unique_pages_dim_accounts, **kwargs):
    try:
        response = requests.get(endpoint, params=params)
        data = response.json()

        if "data" in data:
            for page in data["data"]:
                page_id = page.get("id")  # Unique key for deduplication
                page_name = page.get("name", "Unnamed Page")
                instagram_account = page.get("instagram_business_account", {}).get("id", None)
                instagram_username = get_instagram_username(instagram_account) if instagram_account else None

                # Add to dictionary if not already added
                if page_id not in unique_pages_dim_accounts:
                    unique_pages_dim_accounts[page_id] = {
                        "instagram_account_id": instagram_account,
                        "instagram_account_name": instagram_username,
                        "facebook_page_id": page_id,
                        "facebook_page_name": page_name,
                        "source": source_name
                    }
            logging.info(f"Fetched pages from {source_name}. Total pages: {len(data['data'])}")
        else:
            logging.error(f"Error fetching pages from {source_name}: {data}")
    except Exception as e:
        logging.error(f"Exception in fetch_pages_dim_accounts: {e}")


# Main function to create the dim_instagram_accounts table
def create_dim_instagram_accounts(**kwargs):
    try:
        logging.info("Script started successfully.")
        # Initialize a dictionary to store unique pages (key = page_id)
        unique_pages_dim_accounts = {}
        # print("Script is running")
        # Step 1: Fetch Pages from /owned_pages
        logging.info("Fetching Pages from Owned Pages Endpoint")
        owned_pages_url = f"https://graph.facebook.com/v21.0/{BUSINESS_ACCOUNT_ID}/owned_pages"
        params = {
            "fields": "id,name,instagram_business_account",
            "access_token": ACCESS_TOKEN
        }
        fetch_pages_dim_accounts(owned_pages_url, params, "owned_pages", unique_pages_dim_accounts)

        # Step 2: Fetch Pages from /me/accounts
        logging.info("Fetching Pages from Me Accounts Endpoint")
        accounts_url = "https://graph.facebook.com/v21.0/me/accounts"
        fetch_pages_dim_accounts(accounts_url, params, "me/accounts", unique_pages_dim_accounts)

        # Step 3: Fetch Pages from /client_pages
        logging.info("Fetching Pages from Client Pages Endpoint")
        client_pages_url = f"https://graph.facebook.com/v21.0/{BUSINESS_ACCOUNT_ID}/client_pages"
        fetch_pages_dim_accounts(client_pages_url, params, "client_pages", unique_pages_dim_accounts)

        # Save to CSV
        current_date = datetime.now().strftime("%d_%m_%y")
        df_pages = pd.DataFrame.from_dict(unique_pages_dim_accounts, orient="index")
        df_pages.dropna(subset=["instagram_account_id"], inplace=True)
        df_pages['last_update'] = datetime.now().date()

        return unique_pages_dim_accounts  # Return the dictionary
    except Exception as e:
        logging.error(f"Error: {e}")
        raise
# upload data to snowflake table mako_data_lake.public.dim_instagram_account
# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy.sql import text

# endregion
#region fact_instagram_accounts_daily_data (fetching all facebook pages and their connected instagram account)

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
unique_pages_a = {}


# Function to fetch pages and Instagram accounts
def fetch_pages_fact_accounts(endpoint, params, source_name,unique_pages_a,**kwargs):
    response = requests.get(endpoint, params=params)
    data = response.json()

    if "data" in data:
        for page in data["data"]:
            page_id = page.get("id")  # Unique key for deduplication
            page_name = page.get("name", "Unnamed Page")
            instagram_account = page.get("instagram_business_account", {}).get("id", None)

            # Add to dictionary if not already added
            if page_id not in unique_pages_a:
                unique_pages_a[page_id] = {
                    "facebook_page_name": page_name,
                    # "facebook_page_id": page_id,
                    "instagram_account_id": instagram_account,
                    "source": source_name
                }
    else:
        logging.error(f"Error fetching pages from {source_name}: {data}")


# Function to fetch the Instagram account username
def get_instagram_username_fact_accounts(instagram_account_id):
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
        unique_pages_a = {}
        # Step 1: Fetch Pages from /owned_pages
        logging.info("Fetching Pages from Owned Pages Endpoint")
        owned_pages_url = f"https://graph.facebook.com/v21.0/{BUSINESS_ACCOUNT_ID}/owned_pages"
        params = {
            "fields": "id,name,instagram_business_account",
            "access_token": ACCESS_TOKEN
        }
        fetch_pages_fact_accounts(owned_pages_url, params, "owned_pages", unique_pages_a)

        # Step 2: Fetch Pages from /me/accounts
        logging.info("Fetching Pages from Me Accounts Endpoint")
        accounts_url = "https://graph.facebook.com/v21.0/me/accounts"
        fetch_pages_fact_accounts(accounts_url, params, "me/accounts", unique_pages_a)

        # Step 3: Fetch Pages from /client_pages
        logging.info("Fetching Pages from Client Pages Endpoint")
        client_pages_url = f"https://graph.facebook.com/v21.0/{BUSINESS_ACCOUNT_ID}/client_pages"
        fetch_pages_fact_accounts(client_pages_url, params, "client_pages", unique_pages_a)

        # Step 4: Convert Pages to DataFrame
        df_pages = pd.DataFrame.from_dict(unique_pages_a, orient="index")
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
                instagram_username = get_instagram_username_fact_accounts(instagram_account_id)  # Fetch username
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
#endregion
# region fact_instagram_media_daily_data (fetching all facebook pages and their connected instagram account)

# Date range variables (change these as needed)
START_DATE = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00Z")  # 4 days ago
END_DATE = (datetime.now() - timedelta(days=0)).strftime("%Y-%m-%dT00:00:00Z") #datetime.now().strftime("%Y-%m-%dT23:59:59Z")  # Today

# Define metrics for each media type
# available metrics: https://developers.facebook.com/docs/instagram-platform/reference/instagram-media/insights
REELS_METRICS = [
    "plays",
    "ig_reels_aggregated_all_plays_count",
    "ig_reels_avg_watch_time",
    "ig_reels_video_view_total_time",
    "comments",
    "likes",
    "reach",
    "saved",
    "shares",
    "total_interactions",
    "views "
]

POST_METRICS = [
    "impressions",
    "reach",
    "saved",
    "comments",
    "likes",
    "profile_activity",
    "profile_visits",
    "shares",
    "total_interactions",
    "views "
]

STORY_METRICS = [
    "impressions",
    "reach",
    "replies",
    "shares",
    "total_interactions",
    "views "
]

# Fetch JSON response from a URL asynchronously
async def fetch_json(session, url, params):
    try:
        async with session.get(url, params=params) as response:
            response.raise_for_status()
            return await response.json()
    except Exception as e:
        logging.error(f"Error fetching URL {url}: {e}")
        return {}
# Fetch pages connected to a Facebook business account
async def fetch_pages(session, endpoint, params, source_name):
    pages = []
    data = await fetch_json(session, endpoint, params)

    if "data" in data:
        for page in data["data"]:
            pages.append({
                "page_id": page.get("id"),
                "page_name": page.get("name", "Unnamed Page"),
                "instagram_account_id": page.get("instagram_business_account", {}).get("id"),
                "source": source_name
            })
    else:
        logging.error(f"Error fetching pages from {source_name}: {data}")

    return pages

# Fetch media data for a specific Instagram account within a date range
async def fetch_media(session, instagram_account_id, start_date, end_date):
    media_list = []
    url = f"https://graph.facebook.com/v21.0/{instagram_account_id}/media"
    params = {
        "fields": "id,caption,timestamp,media_type,media_product_type,permalink,thumbnail_url",
        "limit": 50,
        "access_token": ACCESS_TOKEN
    }

    while url:
        data = await fetch_json(session, url, params)

        if "error" in data:
            logging.error(f"Error fetching media: {data['error']}")
            break

        if "data" in data:
            stop_pagination = False
            for media in data["data"]:
                timestamp = media.get("timestamp")
                if timestamp:
                    if timestamp > end_date:
                        continue
                    elif timestamp < start_date:
                        stop_pagination = True
                        break

                    media_list.append({
                        "media_id": str(media.get("id")),
                        "caption": media.get("caption", "No Title"),
                        "timestamp": datetime.strptime(media["timestamp"][:-5], "%Y-%m-%dT%H:%M:%S"),
                        "media_type": media.get("media_type", "Unknown"),
                        "permalink": media.get("permalink", "N/A"),
                        "thumbnail_url": media.get("thumbnail_url", "N/A")
                    })

            if stop_pagination:
                logging.info("Media outside date range detected. Stopping pagination.")
                break
        else:
            logging.info("No data found in response.")
            break

        paging = data.get("paging", {})
        url = paging.get("next", None)

    return media_list

# Fetch insights for a specific media item
async def fetch_media_insights(session, media_id, media_type):
    if media_type == "VIDEO":
        metrics = REELS_METRICS
    elif media_type == "STORY":
        metrics = STORY_METRICS
    elif media_type in ["CAROUSEL_ALBUM", "IMAGE"]:
        metrics = POST_METRICS
    else:
        logging.info(f"Unsupported media type: {media_type}")
        return {}

    url = f"https://graph.facebook.com/v21.0/{media_id}/insights"
    params = {
        "metric": ",".join(metrics),
        "access_token": ACCESS_TOKEN
    }

    data = await fetch_json(session, url, params)

    metrics_data = {metric: 0 for metric in metrics}
    if "data" in data:
        for metric in data["data"]:
            name = metric.get("name")
            value = metric.get("values", [{}])[0].get("value", 0)
            metrics_data[name] = value
    else:
        logging.error(f"Error fetching insights for media ID {media_id}: {data}")

    return metrics_data

# Fetch all media and insights for a specific Instagram account
async def fetch_all_media_and_insights(session, instagram_account_id, page_name, facebook_page_id, start_date, end_date):
    try:
        media_data = await fetch_media(session, instagram_account_id, start_date, end_date)
        tasks = [fetch_media_insights(session, media["media_id"], media["media_type"]) for media in media_data]
        insights_data = await asyncio.gather(*tasks)

        enriched_data = []
        for media, insights in zip(media_data, insights_data):
            enriched_data.append({
                #"facebook_page_name": page_name,
                #"facebook_page_id": facebook_page_id,
                "instagram_account_id": instagram_account_id,
                "media_id": media["media_id"],
                "date_posted": media["timestamp"],
                "title": media["caption"],
                "media_type": media["media_type"],
                "permalink": media["permalink"],
                "thumbnail_url": media["thumbnail_url"],
                **insights
            })

        return enriched_data
    except Exception as e:
        logging.error(f"Error in fetch_all_media_and_insights for account {instagram_account_id}: {e}")
        return []

# Main function to orchestrate fetching and processing of media and insights data
async def create_fact_instagram_media_daily_data():
    try:
        async with aiohttp.ClientSession() as session:
            # Step 1: Fetch Pages from Owned Pages and Me Accounts Endpoints
            owned_pages_url = f"https://graph.facebook.com/v21.0/{BUSINESS_ACCOUNT_ID}/owned_pages"
            me_accounts_url = "https://graph.facebook.com/v21.0/me/accounts"
            client_pages_url = f"https://graph.facebook.com/v21.0/{BUSINESS_ACCOUNT_ID}/client_pages"

            params = {
                "fields": "id,name,instagram_business_account",
                "access_token": ACCESS_TOKEN
            }

            owned_pages = await fetch_pages(session, owned_pages_url, params, "owned_pages")
            me_accounts = await fetch_pages(session, me_accounts_url, params, "me/accounts")
            client_pages = await fetch_pages(session, client_pages_url, params, "client_pages")

            # Combine pages, ensuring no duplicates by instagram_account_id
            all_pages = {page["instagram_account_id"]: page for page in owned_pages + me_accounts + client_pages if page["instagram_account_id"]}.values()

            # Step 2: Fetch Media and Insights for Each Instagram Account
            all_records = []
            for page in all_pages:
                instagram_account_id = page.get("instagram_account_id")
                page_name = page.get("page_name")
                facebook_page_id = page.get("page_id")
                if instagram_account_id:
                    logging.info(f"Fetching media for Instagram account ID: {instagram_account_id}")
                    media_and_insights = await fetch_all_media_and_insights(
                        session, instagram_account_id, page_name, facebook_page_id, START_DATE, END_DATE
                    )
                    all_records.extend(media_and_insights)


            # Step 3: Save all results to a DataFrame and CSV
            #try:
            #    df_media_metrics = pd.DataFrame(all_records)
            #    df_media_metrics['last_update']=datetime.now().date() #adding current date as the table's last update date
            #    logging.info("Combined Media Metrics Data")

        return all_records
    except Exception as e:
        logging.error(f"Error: {e}")
#endregion

def save_to_snowflake(ti, **kwargs):
##########################################################dim_instagram_accounts
    table_dim_accounts = "dim_instagram_accounts"
    table_staging_dim_accounts = "dim_instagram_accounts_staging"
    table_fact_accounts = "fact_instagram_accounts_daily_data"
    table_staging_fact_accounts = "fact_instagram_accounts_daily_data_staging"
    table_fact_media = "fact_instagram_media_daily_data"
    table_staging_fact_media = "fact_instagram_media_daily_data_staging"

    # Pull `unique_pages` from XCom
    unique_pages = ti.xcom_pull(task_ids="create_dim_instagram_accounts")
    if not unique_pages:
        logging.error("dim_accounts(save_to_snowflake function): No data found in XCom for `unique_pages`.")
        raise ValueError("dim_accounts(save_to_snowflake function): No data found in XCom.")

    # Convert Results to a DataFrame
    df_pages = pd.DataFrame.from_dict(unique_pages, orient="index")
    # Debugging logs
    logging.info(f"df_pages columns: {df_pages.columns}")
    logging.info(f"df_pages head: {df_pages.head()}")

    if "instagram_account_id" not in df_pages.columns:
        logging.error("dim_accounts(save_to_snowflake function): The column 'instagram_account_id' is missing in the DataFrame.")
        return

    df_pages.dropna(subset=["instagram_account_id"], inplace=True)
    df_pages['last_update'] = datetime.now().date()
    logging.info(f"dim_accounts(save_to_snowflake function): Combined Pages and Instagram Accounts. Total records: {len(df_pages)}")

    # Connect to Snowflake using SQLAlchemy
    engine = create_engine(
        f"snowflake://{SNOWFLAKE_CONFIG['user']}:{SNOWFLAKE_CONFIG['password']}@{SNOWFLAKE_CONFIG['account']}/"
        f"{SNOWFLAKE_CONFIG['database']}/{SNOWFLAKE_CONFIG['schema']}?warehouse={SNOWFLAKE_CONFIG['warehouse']}"
    )
    # Upload data to dim_instagram_accounts_staging
    try:
        df_pages.to_sql(
            f"{table_staging_dim_accounts}",
            # this staging table is being created when executing this. Its being dropped after the upsert
            con=engine,
            index=False,
            if_exists="replace",
            method="multi",
        )
        logging.info(f"dim_accounts(save_to_snowflake function): Data uploaded successfully to {table_staging_dim_accounts}")
    except Exception as e:
        logging.error(f"dim_accounts(save_to_snowflake function): Error uploading data to staging: {e}")
        raise e

    try:
        Session = sessionmaker(bind=engine)
        session = Session()
        # rows in the staging table
        count_new_rows = text(f"""select count(*) as row_count from {table_staging_dim_accounts}""")
        result = session.execute(count_new_rows)
        row_count = result.scalar()
        logging.info(f"dim_accounts(save_to_snowflake function): Number of rows inserted to {table_staging_dim_accounts}: {row_count}")
        # rows in dim_instagram_accounts before upsert
        count_new_rows_dim = text(f"""select count(*) as row_count from {table_dim_accounts}""")
        result_dim = session.execute(count_new_rows_dim)
        row_count_dim = result_dim.scalar()
        logging.info(f"dim_accounts(save_to_snowflake function): Number of rows exist in {table_dim_accounts} (before upsert): {row_count_dim}")
    except Exception as e:
        logging.error(f"dim_accounts(save_to_snowflake function): Error counting rows in {table_staging_dim_accounts} or {table_dim_accounts}: {e}")
        session.rollback()
    finally:
        session.close()

    # Perform the UPSERT operation using a Snowflake SQL query
    Session = sessionmaker(bind=engine)
    session = Session()

    upsert_query = text(f"""
    MERGE INTO {table_dim_accounts} AS target
    USING {table_staging_dim_accounts} AS source
    ON target.instagram_account_id = source.instagram_account_id --table keys
    WHEN MATCHED THEN
        UPDATE SET 
            INSTAGRAM_ACCOUNT_NAME = source.INSTAGRAM_ACCOUNT_NAME,
            FACEBOOK_PAGE_ID = source.FACEBOOK_PAGE_ID,
            facebook_page_name = source.facebook_page_name,
            source = source.source,
            last_update = source.last_update
    WHEN NOT MATCHED THEN
        INSERT (instagram_account_id, INSTAGRAM_ACCOUNT_NAME, facebook_page_id, facebook_page_name, source, last_update)
        VALUES (source.instagram_account_id, source.INSTAGRAM_ACCOUNT_NAME, source.facebook_page_id, source.facebook_page_name, source.source, source.last_update);
    """)

    try:
        session.execute(upsert_query)
        session.commit()
        logging.info("dim_accounts(save_to_snowflake function): Upsert operation completed successfully.")
    except Exception as e:
        logging.error(f"dim_accounts(save_to_snowflake function): Error during upsert: {e}")
        session.rollback()
        raise e
    finally:
        session.close()

    # Query to count rows in the dim after upsert
    try:
        Session = sessionmaker(bind=engine)
        session = Session()

        count_rows_after_upsert = text(f"""select count(*) as row_count from {table_dim_accounts}""")
        result_after_upsert = session.execute(count_rows_after_upsert)
        row_count_after_upsert = result_after_upsert.scalar()
        logging.info(f"dim_accounts(save_to_snowflake function): Number of rows in {table_dim_accounts} (after upsert): {row_count_after_upsert}")
    except Exception as e:
        logging.error(f"dim_accounts(save_to_snowflake function): Error counting rows in {table_dim_accounts} after upsert: {e}")
        session.rollback()
    finally:
        session.close()

    # Drop the staging table
    try:

        cleanup_query = text \
            (f"DROP TABLE IF EXISTS {SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}.{table_staging_dim_accounts}")
        session.execute(cleanup_query)
        logging.info(f"Table {table_staging_dim_accounts} dropped successfully.")
        session.commit()
    except Exception as e:
        logging.error(f"dim_accounts(save_to_snowflake function): Error dropping staging table: {e}")
        raise e
        session.rollback()
    finally:
        session.close()
#########################################################################################

##############################################################################fact_accounts

    try:
        # Pull `records` from XCom
        records = ti.xcom_pull(task_ids="create_fact_instagram_accounts_daily_data")
        if not records:
            logging.error("fact_accounts(save_to_snowflake function): No data found in XCom for `records`.")
            raise ValueError("fact_accounts(save_to_snowflake function): No data found in XCom.")

        # Convert Results to a DataFrame
        df_metrics = pd.DataFrame(records)
        # Debugging logs
        logging.info(f"fact_accounts(save_to_snowflake function): df_metrics columns: {df_metrics.columns}")
        logging.info(f"fact_accounts(save_to_snowflake function): df_metrics head: {df_metrics.head()}")

        if "instagram_account_id" not in df_metrics.columns:
            logging.error("fact_accounts(save_to_snowflake function): The column 'instagram_account_id' is missing in the DataFrame.")
            return

        df_metrics['last_update'] = datetime.now().date()
    except Exception as e:
        logging.error(f"Error: {e}")

    # Connect to Snowflake using SQLAlchemy
    #engine = create_engine(
    #    f"snowflake://{SNOWFLAKE_CONFIG['user']}:{SNOWFLAKE_CONFIG['password']}@{SNOWFLAKE_CONFIG['account']}/"
    #    f"{SNOWFLAKE_CONFIG['database']}/{SNOWFLAKE_CONFIG['schema']}?warehouse={SNOWFLAKE_CONFIG['warehouse']}"
    #)

    # Upload data to fact_instagram_accounts_daily_data_staging
    try:
        # If the table doesn't exist, create it; otherwise, append data
        df_metrics.to_sql(
            f"{table_staging_fact_accounts}",
            con=engine,
            index=False,  # Do not write the DataFrame index as a separate column
            if_exists="replace",  # Options: 'fail', 'replace', 'append'
            method="multi",  # Optimized bulk insert
        )
        logging.info(f"Data uploaded successfully to {table_staging_fact_accounts}")
    except Exception as e:
        logging.error(f"fact_accounts(save_to_snowflake function): Error uploading data: {e}")
        raise e

        # Query to count rows in the staging table
    try:
        Session = sessionmaker(bind=engine)
        session = Session()
        # rows in the staging table
        count_new_rows = text(f"""select count(*) as row_count from {table_staging_fact_accounts}""")
        result = session.execute(count_new_rows)
        row_count = result.scalar()
        logging.info(f"Number of rows inserted to {table_staging_fact_accounts}: {row_count}")
        # rows in dim_instagram_accounts before upsert
        count_new_rows_dim = text(f"""select count(*) as row_count from {table_fact_accounts}""")
        result_dim = session.execute(count_new_rows_dim)
        row_count_dim = result_dim.scalar()
        logging.info(f"Number of rows exist in {table_fact_accounts} (before upsert): {row_count_dim}")
    except Exception as e:
        logging.error(f"Error counting rows in {table_staging_fact_accounts} or {table_fact_accounts}: {e}")
        session.rollback()
    finally:
        session.close()

    # Perform the UPSERT operation using a Snowflake SQL query
    Session = sessionmaker(bind=engine)
    session = Session()

    upsert_query = text(f"""
    MERGE INTO {table_fact_accounts} AS target
    USING {table_staging_fact_accounts} AS source
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
        logging.info("fact_accounts(save_to_snowflake function): Upsert operation completed successfully.")
    except Exception as e:
        logging.error(f"fact_accounts(save_to_snowflake function): Error during upsert: {e}")
        raise e
        session.rollback()
    finally:
        session.close()

    # Query to count rows in the fact after upsert
    try:
        Session = sessionmaker(bind=engine)
        session = Session()

        count_rows_after_upsert = text(f"""select count(*) as row_count from {table_fact_accounts}""")
        result_after_upsert = session.execute(count_rows_after_upsert)
        row_count_after_upsert = result_after_upsert.scalar()
        logging.info(f"Number of rows in {table_fact_accounts} (after upsert): {row_count_after_upsert}")
    except Exception as e:
        logging.error(f"Error counting rows in {table_fact_accounts} after upsert: {e}")
        session.rollback()
    finally:
        session.close()

    try:
        # Update table_last_update to the current date
        update_table_last_update_query = text(f"""
        UPDATE {table_fact_accounts}
        SET table_last_update = :current_date
        """)
        session.execute(update_table_last_update_query, {"current_date": datetime.now().date()})
        session.commit()
        logging.info("fact_accounts(save_to_snowflake function): table_last_update column updated successfully.")
    except Exception as e:
        logging.error(f"fact_accounts(save_to_snowflake function): Error updating table_last_update: {e}")
        session.rollback()
    finally:
        session.close()

    # Drop the staging table
    try:

        cleanup_query = text(f"DROP TABLE IF EXISTS {SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}.{table_staging_fact_accounts}")
        session.execute(cleanup_query)
        logging.info(f"Table {table_staging_fact_accounts} dropped successfully.")
        session.commit()
    except Exception as e:
        logging.error(f"fact_accounts(save_to_snowflake function): Error dropping staging table: {e}")
        raise e
        session.rollback()
    finally:
        session.close()
########################################################################

#########################################################################fact_media

    try:
        # Pull `all_records` from XCom
        all_records = ti.xcom_pull(task_ids="create_fact_instagram_media_daily_data")
        if not all_records:
            logging.error("fact_media(save_to_snowflake function): No data found in XCom for `all_records`.")
            raise ValueError("fact_media(save_to_snowflake function): No data found in XCom.")

        # Convert Results to a DataFrame
        df_media_metrics = pd.DataFrame(all_records)
        # Debugging logs
        logging.info(f"fact_media(save_to_snowflake function): df_media_metrics columns: {df_media_metrics.columns}")
        logging.info(f"fact_media(save_to_snowflake function): df_media_metrics head: {df_media_metrics.head()}")

        if "instagram_account_id" not in df_media_metrics.columns:
            logging.error("fact_media(save_to_snowflake function): The column 'instagram_account_id' is missing in the DataFrame.")
            return
        df_media_metrics['last_update'] = datetime.now().date()  # adding current date as the table's last update date
        logging.info("fact_media(save_to_snowflake function): Combined Media Metrics Data")
    except Exception as e:
        logging.error(f"fact_media(save_to_snowflake function): Error combining media metrics data: {e}")

    # Connect to Snowflake using SQLAlchemy
    #engine = create_engine(
    #    f"snowflake://{SNOWFLAKE_CONFIG['user']}:{SNOWFLAKE_CONFIG['password']}@{SNOWFLAKE_CONFIG['account']}/"
    #    f"{SNOWFLAKE_CONFIG['database']}/{SNOWFLAKE_CONFIG['schema']}?warehouse={SNOWFLAKE_CONFIG['warehouse']}"
    #)

    # Upload data to fact_instagram_accounts_daily_data_staging
    try:
        # If the table doesn't exist, create it; otherwise, append data
        df_media_metrics.to_sql(
            f"{table_staging_fact_media}",
            con=engine,
            index=False,  # Do not write the DataFrame index as a separate column
            if_exists="replace",  # Options: 'fail', 'replace', 'append'
            method="multi",  # Optimized bulk insert
        )
        logging.info(f"Data uploaded successfully to {table_staging_fact_media}")
    except Exception as e:
        logging.error(f"fact_media(save_to_snowflake function): Error uploading data: {e}")
        raise e

    try:
        Session = sessionmaker(bind=engine)
        session = Session()
        # rows in the staging table
        count_new_rows = text(f"""select count(*) as row_count from {table_staging_fact_media}""")
        result = session.execute(count_new_rows)
        row_count = result.scalar()
        logging.info(f"Number of rows inserted to {table_staging_fact_media}: {row_count}")
        # rows in fact before upsert
        count_new_rows_dim = text(f"""select count(*) as row_count from {table_fact_media}""")
        result_dim = session.execute(count_new_rows_dim)
        row_count_dim = result_dim.scalar()
        logging.info(f"Number of rows exist in {table_fact_media} (before upsert): {row_count_dim}")
    except Exception as e:
        logging.error(f"Error counting rows in {table_staging_fact_media} or {table_fact_media}: {e}")
        session.rollback()
    finally:
        session.close()

    # Perform the UPSERT operation using a Snowflake SQL query
    Session = sessionmaker(bind=engine)
    session = Session()

    upsert_query = text(f"""
    MERGE INTO {table_fact_media} AS target
    USING {table_staging_fact_media} AS source
    ON target.instagram_account_id = source.instagram_account_id and target.media_id = source.media_id --table keys
    WHEN MATCHED THEN
        UPDATE SET 
            date_posted = source.date_posted,
            title = source.title,
            media_type = source.media_type,
            permalink = source.permalink,
            thumbnail_url = source.thumbnail_url,
            impressions = source.impressions,
            reach = source.reach,
            saved = source.saved,
            comments = source.comments,
            likes = source.likes,
            profile_activity = source.profile_activity,
            profile_visits = source.profile_visits,
            shares = source.shares,
            total_interactions = source.total_interactions,
            plays = source.plays,
            ig_reels_aggregated_all_plays_count = source.ig_reels_aggregated_all_plays_count,
            ig_reels_avg_watch_time = source.ig_reels_avg_watch_time,
            ig_reels_video_view_total_time = source.ig_reels_video_view_total_time,
            last_update = source.last_update,
            views = source.views 
    WHEN NOT MATCHED THEN
        INSERT (instagram_account_id, media_id, date_posted, title, media_type, permalink, thumbnail_url, impressions, reach, saved, comments, likes, profile_activity, profile_visits, shares, total_interactions, plays, ig_reels_aggregated_all_plays_count, ig_reels_avg_watch_time, ig_reels_video_view_total_time, last_update, views )
        VALUES (source.instagram_account_id, source.media_id, source.date_posted, source.title, source.media_type, source.permalink, source.thumbnail_url, source.impressions, source.reach, source.saved, source.comments, source.likes, source.profile_activity, source.profile_visits, source.shares, source.total_interactions, source.plays, source.ig_reels_aggregated_all_plays_count, source.ig_reels_avg_watch_time, source.ig_reels_video_view_total_time, source.last_update, source.views);
    """)

    try:
        session.execute(upsert_query)
        session.commit()
        logging.info("fact_media(save_to_snowflake function): Upsert operation completed successfully.")
    except Exception as e:
        logging.error(f"fact_media(save_to_snowflake function): Error during upsert: {e}")
        raise e
        session.rollback()
    finally:
        session.close()

    # Query to count rows in the fact after upsert
    try:
        Session = sessionmaker(bind=engine)
        session = Session()

        count_rows_after_upsert = text(f"""select count(*) as row_count from {table_fact_media}""")
        result_after_upsert = session.execute(count_rows_after_upsert)
        row_count_after_upsert = result_after_upsert.scalar()
        logging.info(f"Number of rows in {table_fact_media} (after upsert): {row_count_after_upsert}")
    except Exception as e:
        logging.error(f"Error counting rows in {table_fact_media} after upsert: {e}")
        session.rollback()
    finally:
        session.close()

    try:
        # Update table_last_update to the current date
        update_table_last_update_query = text(f"""
        UPDATE {table_fact_media}
        SET table_last_update = :current_date
        """)
        session.execute(update_table_last_update_query, {"current_date": datetime.now().date()})
        session.commit()
        logging.info("fact_media(save_to_snowflake function): table_last_update column updated successfully.")
    except Exception as e:
        logging.error(f"fact_media(save_to_snowflake function): Error updating table_last_update: {e}")
        session.rollback()
    finally:
        session.close()

    # Drop the staging table
    try:

        cleanup_query = text(f"DROP TABLE IF EXISTS {SNOWFLAKE_CONFIG['database']}.{SNOWFLAKE_CONFIG['schema']}.{table_staging_fact_media}")
        session.execute(cleanup_query)
        logging.info(f"Table {table_staging_fact_media} dropped successfully.")
        session.commit()
    except Exception as e:
        logging.error(f"fact_media(save_to_snowflake function): Error dropping staging table: {e}")
        raise e
        session.rollback()
    finally:
        session.close()

def run_async_fetch():
    # Run the async function in the event loop
    return asyncio.run(create_fact_instagram_media_daily_data())


#######################

default_args = {
    'owner': 'airflow_omer',
    # 'start_date':datetime(2024, 1, 7),
    # 'email': ['omer.yarchi@keshet-d.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=3)
}

with DAG(
        dag_id="instagram_data_to_snowflake",
        default_args=default_args,
        description="Fetch Instagram accounts and media data and upload to Snowflake",
        schedule_interval="0 15 * * *",  # Run daily at 17:00
        start_date=datetime(2025, 1, 7),
        catchup=False,
) as dag:

    fetch_task_dim_accounts = PythonOperator(
        task_id="create_dim_instagram_accounts",
        python_callable=create_dim_instagram_accounts,
        on_failure_callback=send_slack_error_notification,
        on_success_callback=send_slack_success_notification
    )

    fetch_task_fact_accounts = PythonOperator(
        task_id="create_fact_instagram_accounts_daily_data",
        python_callable=create_fact_instagram_accounts_daily_data,
        on_failure_callback=send_slack_error_notification,
        on_success_callback=send_slack_success_notification
    )

    fetch_task_fact_media = PythonOperator(
        task_id="create_fact_instagram_media_daily_data",
        python_callable=run_async_fetch,
        on_failure_callback=send_slack_error_notification,
        on_success_callback=send_slack_success_notification
    )

    save_task = PythonOperator(
        task_id="save_to_snowflake",
        python_callable=save_to_snowflake,
        on_failure_callback=send_slack_error_notification,
        on_success_callback=send_slack_success_notification
    )

    [fetch_task_dim_accounts, fetch_task_fact_accounts, fetch_task_fact_media] >> save_task


