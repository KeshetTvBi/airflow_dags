#10154132454588797/owned_pages?fields=id,name
#me/accounts?fields=id,name
#me/accounts?fields=id,name,access_token&access_token=EAAMocfT7JiABO9ewi0Tx9zMojbLqSULSd3mIRqJtdkpzCUNmvsZC9fkhDvcZAVZCC6MUuQ3tZB9iZBxFc4d8SXx0IvUvpbR4N32vCDEXVnMLrxiS1e21ZApzNi5BJLXjVrUqnzmTHZAooJLUfU6sCZAHiHwn2eeeIOpZCFvZANT8gI7QNf3SnFacipLdCfTaktrhjJrJZAirZB1SfoZCrhRIn9pcQx1aZATQZDZD
#https://developers.facebook.com/docs/graph-api/reference/v22.0/insights#page-views

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
ACCESS_TOKEN = 'EAAMocfT7JiABOyva52WpdZCZA41er1msE3EuBxBLeosyQ0aKZBl01KHU9c5lQQSvDd0QtA7ts8Bd3QDarbFb0hdlHFR01MmmYjnAFsYZBdXtyPQQfL7BPmYQOENYjWOJ0kZCVqU1ZA9ZBq6XEaWE5ZCRWSvVRlVCnUW5f1kQBmJfEfCRlECmDCtxX6fVwzZC2d3ox'
# access token expire on april 10 2025. to extend go here https://developers.facebook.com/tools/debug/accesstoken/
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


#region dim_facebook_accounts
unique_pages_dim_accounts = {}
def fetch_facebook_pages(source, url):
    """Fetch Facebook pages and return a list of dictionaries"""
    try:
        logging.info(f"Fetching Facebook Pages from {source}")
        response = requests.get(url).json()

        if "data" in response:
            return [
                {
                    "facebook_account_id": page["id"],
                    "facebook_account_name": page.get("name", "Unnamed Page"),
                    "instagram_account_id": page.get("instagram_business_account", {}).get("id"),
                    "source": source
                }
                for page in response["data"]
            ]
        else:
            logging.error(f"Error fetching pages from {source}: {response}")
            return []
    except Exception as e:
        logging.error(f"Error in fetch_facebook_pages: {e}")
        return []

def create_dim_facebook_accounts():
    """Fetch all Facebook pages and store in a dictionary"""
    try:
        logging.info("Script started successfully.")
        # API endpoints
        user_pages_url = f"https://graph.facebook.com/v22.0/me/accounts?fields=id,name,instagram_business_account&access_token={ACCESS_TOKEN}"
        business_pages_url = f"https://graph.facebook.com/v22.0/{BUSINESS_ACCOUNT_ID}/owned_pages?fields=id,name,instagram_business_account&access_token={ACCESS_TOKEN}"

        # Fetch and merge data from multiple endpoints
        all_pages = fetch_facebook_pages("me/accounts", user_pages_url) + fetch_facebook_pages("business/owned_pages", business_pages_url)

        # Convert list to dictionary using page ID as the key
        unique_pages_dim_accounts = {page["facebook_account_id"]: page for page in all_pages}

        # Convert to DataFrame (if needed)
        df_dim_pages = pd.DataFrame.from_dict(unique_pages_dim_accounts, orient="index")
        df_dim_pages['last_update'] = datetime.now().date()

        # Save to CSV
        #df_dim_pages.to_csv(r"C:\Users\omer.yarchi\Desktop\Graph API for Facebook Insights\dim_facebook_accounts\dim_facebook_accounts.csv", index=False, encoding="utf-8-sig")

        return unique_pages_dim_accounts  # Return the final dictionary

    except Exception as e:
        logging.error(f"Error: {e}")
        raise

#endregion
#region fact_facebook_account_daily_data

# Define the metrics to fetch
metrics = [
    "page_impressions",
    "page_impressions_unique",
    "page_impressions_paid",
    "page_impressions_paid_unique",
    "page_posts_impressions",
    "page_posts_impressions_unique",
    "page_posts_impressions_paid",
    "page_posts_impressions_paid_unique",
    "page_posts_impressions_organic_unique",
    "page_daily_follows_unique",
    "page_daily_unfollows_unique",
    "page_follows",
    "page_post_engagements",
    "page_views_total"

]


#Function to fetch data from the api endpoints
async def fetch_data_facebook(session, url):
    async with session.get(url) as response:
        response_json = await response.json()
        if "error" in response_json:
            logging.warning(f"API Error: {response_json['error']['message']}")
        return response_json

# Function to get Facebook Pages and their Access Tokens (we need the page access token to retrieve the page metrics, unlike instagram where we can use user token)
async def get_pages_with_tokens(session, url):
    response_json = await fetch_data_facebook(session, url) #Use rate-limited call

    # Check if "data" exists and contains pages
    if "data" not in response_json or not response_json["data"]:
        return []  # Return empty list if no data is found

    return [
        {
            "facebook_account_id": page["id"],
            "page_access_token": page["access_token"]
        }
        for page in response_json["data"] if "access_token" in page
    ]

# Function to fetch insights for a page using its Page Access Token
async def get_page_insights_facebook(session, page_id, page_token, start_date, end_date):
    logging.info(f"Fetching insights for Page ID: {page_id}")

    url = f"https://graph.facebook.com/v22.0/{page_id}/insights?metric={','.join(metrics)}&period=day&since={start_date}&until={end_date}&access_token={page_token}"

    response_json = await fetch_data_facebook(session, url)  #Use rate-limited call

    if "error" in response_json:
        logging.error(f"⚠️ API Error: {response_json['error']['message']}")
        return []

    insights_list = []
    if "data" in response_json and response_json["data"]:
        for metric_data in response_json["data"]:
            metric_name = metric_data["name"]
            if "values" in metric_data and metric_data["values"]:
                for entry in metric_data["values"]:
                    insights_list.append({
                        "date": datetime.strptime(entry.get("end_time"),'%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d'),
                        "facebook_account_id": page_id,
                        metric_name: entry.get("value")
                    })

    return insights_list

# Main async function to handle all API calls concurrently
async def create_fact_facebook_accounts_daily_data():
    async with aiohttp.ClientSession() as session:
        # Fetch pages from both endpoints concurrently
        user_pages_url = f"https://graph.facebook.com/v22.0/me/accounts?fields=id,access_token&access_token={ACCESS_TOKEN}"
        business_pages_url = f"https://graph.facebook.com/v22.0/{BUSINESS_ACCOUNT_ID}/owned_pages?fields=id,access_token&access_token={ACCESS_TOKEN}"

        user_pages = await get_pages_with_tokens(session, user_pages_url)
        business_pages = await get_pages_with_tokens(session, business_pages_url)

        all_pages = user_pages + business_pages  # Merge results

        # Set the date range. cant be more than 90 days
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S")
        end_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        # Fetch insights for all pages concurrently
        tasks = [
            get_page_insights_facebook(session, page["facebook_account_id"], page["page_access_token"], start_date, end_date)
            for page in all_pages
        ]
        all_insights = await asyncio.gather(*tasks)

        # Flatten list of lists
        flattened_insights = [entry for sublist in all_insights for entry in sublist]

        # Convert list of insights into DataFrame
        df_pages = pd.DataFrame(flattened_insights)

        if df_pages.empty:
            logging.error("⚠️ No page insights retrieved. Skipping pivot step.")
        else:
        # Ensure all metrics exist (fill missing values with NaN)
            df_pages = df_pages.pivot_table(index=["date", "facebook_account_id"], aggfunc="first").reset_index()

        # Add last_update column
        df_pages["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        fact_page_insights=df_pages.to_dict(orient="records")


        # Save to CSV
        #df_pages.to_csv(
        #    r"C:\Users\omer.yarchi\Desktop\Graph API for Facebook Insights\fact_facebook_accounts_daily_data\fact_facebook_accounts_daily_data.csv",
        #    index=False, encoding='utf-8-sig')

    return fact_page_insights



#endregion fact_facebook_account_daily_data
#region fact_facebook_posts_daily_data

#metrics
post_metrics = [
    "post_impressions",
    "post_impressions_unique",
    "post_engagements",
    "post_clicks",
    "post_reactions_like_total",
    "post_reactions_love_total",
    "post_reactions_wow_total",
    "post_reactions_haha_total",
    "post_reactions_sorry_total",
    "post_reactions_anger_total"
]

video_metrics = [
    "post_video_views",
    "post_video_views_unique",
    "post_video_avg_time_watched",
    "post_video_complete_views_organic",
    "post_video_complete_views_organic_unique",
    "post_video_complete_views_paid",
    "post_video_complete_views_paid_unique",
    "post_video_views_organic",
    "post_video_views_organic_unique",
    "post_video_views_paid",
    "post_video_views_paid_unique",
    "post_video_views_15s"
]

#retrieving post ids for all pages
async def get_posts_for_page_facebook(session, page_id, page_token, start_date, end_date):
    posts=[]
    url = f"https://graph.facebook.com/v22.0/{page_id}/feed?fields=id,created_time,message&since={start_date}&until={end_date}&limit=100&access_token={page_token}"

    while url:
        #print(f"🔗 Requesting posts for page {page_id}: {url}")
        response_json = await fetch_data_facebook(session, url)  # ✅ Use rate-limited call

        if "error" in response_json:
            print(f"⚠️ API Error: {response_json['error']['message']}")
            return []

        if "data" in response_json and response_json["data"]:
            for post in response_json["data"]:
                posts.append({
                    "post_id": post["id"],
                    "post_title": post.get("message", ""),
                    "facebook_account_id": page_id,
                    "date_posted": post["created_time"],
                    "page_access_token": page_token
                })

        # Check for next page
        url = response_json.get("paging", {}).get("next")


    return posts

#loop over post IDs and retrieve insights for each
async def get_post_insights_facebook(session, post_id, page_id,date_posted, start_date, end_date, page_token):
    fields_url = f"https://graph.facebook.com/v22.0/{post_id}?fields=shares.summary(true),likes.summary(true),comments.summary(true),reactions.summary(true),story,status_type,message,created_time,permalink_url&access_token={page_token}"
    fields_response = await fetch_data_facebook(session, fields_url)

    if "error" in fields_response :
        logging.error(f"⚠️ API Error: {fields_response ['error']['message']}")
        return []

    post_type = fields_response.get("status_type", "unknown")  # Check post type

    # Choose appropriate metrics based on post type
    metrics_to_fetch = post_metrics
    if post_type == "added_video":
        metrics_to_fetch = video_metrics  # Include video-specific metrics

    # Fetch insights
    insights_dict = {}
    insights_url = f"https://graph.facebook.com/v22.0/{post_id}/insights?metric={','.join(metrics_to_fetch)}&limit=100&access_token={page_token}"
    insights_response = await fetch_data_facebook(session, insights_url)
    while insights_url:
        insights_response = await fetch_data_facebook(session, insights_url)
        if "error" in insights_response :
            logging.error(f"⚠️ API Error: {insights_response ['error']['message']}")
            return []

        #insights = {"facebook_account_id": page_id, "post_id": post_id}
        if "data" in insights_response and insights_response ["data"]:
            for metric_data in insights_response.get("data", []):
                metric_name = metric_data["name"]

                if "values" in metric_data and metric_data["values"]:
                    # Separate daily vs. lifetime values
                    daily_values = {entry["end_time"]: entry["value"] for entry in metric_data["values"] if
                                    "end_time" in entry}
                    lifetime_values = [entry["value"] for entry in metric_data["values"] if "end_time" not in entry]

                    # Pick the latest daily value if available
                    if daily_values:
                        latest_end_time = max(daily_values.keys())  # Get the most recent date
                        latest_value = daily_values[latest_end_time]  # Use the corresponding value
                    elif lifetime_values:
                        latest_value = lifetime_values[0]  # Use lifetime if no daily available
                    else:
                        latest_value = None  # No data

                    # Store only the latest value per metric in the dictionary
                    if latest_value is not None:
                        insights_dict[metric_name] = latest_value
        # ✅ Handle Pagination for Insights
        insights_url = insights_response.get("paging", {}).get("next")

        # Process additional fields
        post_details = fields_response
        post_info = {
            "date_posted": post_details.get("created_time"),
            "facebook_account_id": page_id,
            "post_id": post_id,
            "shares": post_details.get("shares", {}).get("count", 0),
            "likes": post_details.get("likes", {}).get("summary", {}).get("total_count", 0),
            "comments": post_details.get("comments", {}).get("summary", {}).get("total_count", 0),
            "reactions": post_details.get("reactions", {}).get("summary", {}).get("total_count", 0),
            "message": post_details.get("message", "N/A"),
            "story": post_details.get("story", "N/A"),
            "type": post_details.get("status_type", "N/A")
        }
        #print(f"✅ Adding Post Info: {post_info}")
        insights_row = {**post_info, **insights_dict}

    return [insights_row] #insights  # Dictionary with post metrics


#main
async def create_fact_facebook_media_daily_data():
    async with aiohttp.ClientSession() as session:
        user_pages_url = f"https://graph.facebook.com/v22.0/me/accounts?fields=id,access_token&access_token={ACCESS_TOKEN}"
        business_pages_url = f"https://graph.facebook.com/v22.0/{BUSINESS_ACCOUNT_ID}/owned_pages?fields=id,access_token&access_token={ACCESS_TOKEN}"

        user_pages = await get_pages_with_tokens(session, user_pages_url)
        business_pages = await get_pages_with_tokens(session, business_pages_url)
        all_pages = business_pages + user_pages

        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S")
        end_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        # 1️⃣ Fetch all posts for each page concurrently
        post_tasks = [get_posts_for_page_facebook(session, page["facebook_account_id"], page["page_access_token"], start_date, end_date) for page in all_pages]
        all_posts = await asyncio.gather(*post_tasks)

        # Flatten list of lists
        flattened_posts = [post for sublist in all_posts for post in sublist]
        # 2️⃣ Fetch insights for all posts concurrently
        post_insight_tasks = [
            get_post_insights_facebook(session, post["post_id"], post["facebook_account_id"], post["date_posted"],start_date, end_date, page["page_access_token"])
            for post in flattened_posts for page in all_pages if post["facebook_account_id"] == page["facebook_account_id"]
        ]
        all_post_insights = await asyncio.gather(*post_insight_tasks)

        # Convert list of insights into DataFrame
        # Flatten `all_post_insights` before converting to a DataFrame
        flattened_post_insights = [item for sublist in all_post_insights for item in sublist]  # Flatten nested lists
        df_posts = pd.DataFrame(flattened_post_insights)
        if df_posts.empty:
            print("⚠️ No post insights retrieved. Skipping pivot step.")
        else:
            #print("\n📊 Checking DataFrame before pivoting:")
            #print(df_posts.head(10))  # Check the first 10 rows
            #print(df_posts.columns)  # Check column names
            df_posts = df_posts.pivot_table(index=["date_posted", "facebook_account_id", "post_id"],
                                           aggfunc="first").reset_index()
            #df_posts["date_posted"].fillna(df_posts.groupby("post_id")["date_posted"].transform("first"))

            #df_posts = df_posts.pivot_table(index=["date_posted", "facebook_account_id", "post_id"],
                                            #aggfunc="first").reset_index()
            #print(df_posts.columns)

        # Ensure all metrics exist (fill missing values with NaN)
        #df_posts = df_posts.pivot_table(index=["date_posted", "facebook_account_id", "post_id"], aggfunc="first").reset_index()

        # Add last_update column
        df_posts["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        #reformat date posted
        df_posts["date_posted"] = pd.to_datetime(df_posts["date_posted"])
        df_posts["date_posted"] = df_posts["date_posted"].dt.strftime("%Y-%m-%d %H:%M:%S")
        df_posts["post_id"] = df_posts["post_id"].astype(str).str.split("_").str[-1]
        post_insights_dict = df_posts.to_dict(orient="records")



        # Save to CSV
        #df_posts.to_csv(r"C:\Users\omer.yarchi\Desktop\Graph API for Facebook Insights\fact_facebook_posts_daily_data\fact_facebook_posts_daily_data.csv", index=False, encoding='utf-8-sig')
        return post_insights_dict
#endregion
#region save to snowflake

def save_to_snowflake_facebook(ti, **kwargs):
##########################################################dim_instagram_accounts
    table_dim_accounts = "dim_facebook_accounts"
    table_staging_dim_accounts = "dim_facebook_accounts_staging"
    table_fact_accounts = "fact_facebook_accounts_daily_data"
    table_staging_fact_accounts = "fact_facebook_accounts_daily_data_staging"
    table_fact_media = "fact_facebook_posts_daily_data"
    table_staging_fact_media = "fact_facebook_posts_daily_data_staging"
    # Pull `unique_pages` from XCom
    unique_pages_dim_accounts = ti.xcom_pull(task_ids="create_dim_facebook_accounts")
    if not unique_pages_dim_accounts:
        logging.error("dim_accounts(save_to_snowflake function): No data found in XCom for `unique_pages_dim_accounts`.")
        raise ValueError("dim_accounts(save_to_snowflake function): No data found in XCom.")

    # Convert Results to a DataFrame
    df_pages = pd.DataFrame.from_dict(unique_pages_dim_accounts, orient="index")
    # Debugging logs
    logging.info(f"df_pages columns: {df_pages.columns}")
    logging.info(f"df_pages head: {df_pages.head()}")

    if "facebook_account_id" not in df_pages.columns:
        logging.error("dim_accounts(save_to_snowflake function): The column 'facebook_account_id' is missing in the DataFrame.")
        return

    df_pages.dropna(subset=["facebook_account_id"], inplace=True)
    df_pages['last_update'] = datetime.now().date()
    logging.info(f"dim_accounts(save_to_snowflake function): Combined Pages and Facebook Accounts. Total records: {len(df_pages)}")

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
    ON target.facebook_account_id = source.facebook_account_id --table keys
    WHEN MATCHED THEN
        UPDATE SET 
            FACEBOOK_ACCOUNT_ID = source.FACEBOOK_ACCOUNT_ID,
            FACEBOOK_ACCOUNT_NAME = source.FACEBOOK_ACCOUNT_NAME,
            INSTAGRAM_ACCOUNT_ID = source.INSTAGRAM_ACCOUNT_ID,
            source = source.source,
            last_update = source.last_update
    WHEN NOT MATCHED THEN
        INSERT (FACEBOOK_ACCOUNT_ID, FACEBOOK_ACCOUNT_NAME, INSTAGRAM_ACCOUNT_ID, source, last_update)
        VALUES (source.FACEBOOK_ACCOUNT_ID, source.FACEBOOK_ACCOUNT_NAME, source.INSTAGRAM_ACCOUNT_ID, source.source, source.last_update);
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

######################################################################################
#######fact_accounts
##############################################################################fact_accounts

    try:
        # Pull `records` from XCom
        fact_page_insights = ti.xcom_pull(task_ids="create_fact_facebook_accounts_daily_data")
        if not fact_page_insights:
            logging.error("fact_accounts(save_to_snowflake function): No data found in XCom for `fact_page_insights`.")
            raise ValueError("fact_accounts(save_to_snowflake function): No data found in XCom.")

        # Convert Results to a DataFrame
        df_metrics = pd.DataFrame(fact_page_insights)
        # Debugging logs
        logging.info(f"fact_accounts(save_to_snowflake function): df_metrics columns: {df_metrics.columns}")
        logging.info(f"fact_accounts(save_to_snowflake function): df_metrics head: {df_metrics.head()}")

        if "facebook_account_id" not in df_metrics.columns:
            logging.error("fact_accounts(save_to_snowflake function): The column 'facebook_account_id' is missing in the DataFrame.")
            return

        df_metrics.dropna(subset=["facebook_account_id"], inplace=True)
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
    ON target.facebook_account_id = source.facebook_account_id and target.date = source.date --table keys
    WHEN MATCHED THEN
        UPDATE SET 
            page_daily_follows_unique = source.page_daily_follows_unique,
            page_daily_unfollows_unique = source.page_daily_unfollows_unique,
            page_follows = source.page_follows,
            page_impressions = source.page_impressions,
            page_impressions_paid = source.page_impressions_paid,
            page_impressions_paid_unique = source.page_impressions_paid_unique,
            page_impressions_unique = source.page_impressions_unique,
            page_post_engagements = source.page_post_engagements,
            page_posts_impressions = source.page_posts_impressions,
            page_posts_impressions_organic_unique = source.page_posts_impressions_organic_unique,
            page_posts_impressions_paid = source.page_posts_impressions_paid,
            page_posts_impressions_paid_unique = source.page_posts_impressions_paid_unique,
            page_posts_impressions_unique = source.page_posts_impressions_unique,
            page_views_total = source.page_views_total,
            last_update = source.last_update
    WHEN NOT MATCHED THEN
        INSERT (facebook_account_id, date, page_daily_follows_unique, page_daily_unfollows_unique, page_follows, page_impressions,page_impressions_paid,page_impressions_paid_unique,page_post_engagements,page_posts_impressions,page_posts_impressions_organic_unique,page_posts_impressions_paid,page_posts_impressions_paid_unique,page_posts_impressions_unique,page_views_total,last_update)
        VALUES (source.facebook_account_id, source.date, source.page_daily_follows_unique, source.page_daily_unfollows_unique, source.page_follows, source.page_impressions, source.page_impressions_paid, source.page_impressions_paid_unique, source.page_post_engagements, source.page_posts_impressions, source.page_posts_impressions_organic_unique, source.page_posts_impressions_paid, source.page_posts_impressions_paid_unique, source.page_posts_impressions_unique, source.page_views_total, source.last_update);
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
        SET last_update = :current_date
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
###media
#########################################################################fact_media

    try:
        # Pull `all_records` from XCom
        post_insights_dict = ti.xcom_pull(task_ids="create_fact_facebook_media_daily_data")
        if not post_insights_dict:
            logging.error("fact_media(save_to_snowflake function): No data found in XCom for `post_insights_dict`.")
            raise ValueError("fact_media(save_to_snowflake function): No data found in XCom.")

        # Convert Results to a DataFrame
        df_media_metrics = pd.DataFrame(post_insights_dict)
        # Debugging logs
        logging.info(f"fact_media(save_to_snowflake function): df_media_metrics columns: {df_media_metrics.columns}")
        logging.info(f"fact_media(save_to_snowflake function): df_media_metrics head: {df_media_metrics.head()}")

        if "facebook_account_id" not in df_media_metrics.columns:
            logging.error("fact_media(save_to_snowflake function): The column 'facebook_account_id' is missing in the DataFrame.")
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
    ON target.facebook_account_id = source.facebook_account_id and target.post_id = source.post_id --table keys
    WHEN MATCHED THEN
        UPDATE SET 
            date_posted = source.date_posted,
            message = source.message,
            type = source.type,
            comments = source.comments,
            likes = source.likes,
            reactions = source.reactions,
            shares = source.shares,
            post_clicks = source.post_clicks,
            post_impressions = source.post_impressions,
            post_impressions_unique = source.post_impressions_unique,
            post_reactions_anger_total = source.post_reactions_anger_total,
            post_reactions_haha_total = source.post_reactions_haha_total,
            post_reactions_like_total = source.post_reactions_like_total,
            post_reactions_love_total = source.post_reactions_love_total,
            post_reactions_sorry_total = source.post_reactions_sorry_total,
            post_reactions_wow_total = source.post_reactions_wow_total,
            post_video_avg_time_watched = source.post_video_avg_time_watched,
            post_video_complete_views_organic = source.post_video_complete_views_organic,
            post_video_complete_views_organic_unique = source.post_video_complete_views_organic_unique,
            post_video_complete_views_paid = source.post_video_complete_views_paid,
            post_video_complete_views_paid_unique = source.post_video_complete_views_paid_unique,
            post_video_views = source.post_video_views,
            post_video_views_15s = source.post_video_views_15s,
            post_video_views_organic = source.post_video_views_organic,
            post_video_views_organic_unique = source.post_video_views_organic_unique,
            post_video_views_paid = source.post_video_views_paid,
            post_video_views_paid_unique = source.post_video_views_paid_unique,
            post_video_views_unique = source.post_video_views_unique,
            story = source.story,
            last_update = source.last_update
    WHEN NOT MATCHED THEN
        INSERT (facebook_account_id, post_id, date_posted, message, type, comments, likes, reactions, shares, post_clicks, post_impressions, post_impressions_unique, post_reactions_anger_total, post_reactions_haha_total, post_reactions_like_total, post_reactions_love_total, post_reactions_sorry_total, post_reactions_wow_total, post_video_avg_time_watched, post_video_complete_views_organic,post_video_complete_views_organic_unique, post_video_complete_views_paid, post_video_complete_views_paid_unique,post_video_views,post_video_views_15s, post_video_views_organic,post_video_views_organic_unique,post_video_views_paid,post_video_views_paid_unique,post_video_views_unique,story,last_update )
        VALUES (source.facebook_account_id, source.post_id, source.date_posted, source.message, source.type, source.comments, source.likes, source.reactions, source.shares, source.post_clicks, source.post_impressions, source.post_impressions_unique, source.post_reactions_anger_total, source.post_reactions_haha_total, source.post_reactions_like_total, source.post_reactions_love_total, source.post_reactions_sorry_total, source.post_reactions_wow_total, source.post_video_avg_time_watched, source.post_video_complete_views_organic,source.post_video_complete_views_organic_unique,source.post_video_complete_views_paid,source.post_video_complete_views_paid_unique,source.post_video_views,source.post_video_views_15s, source.post_video_views_organic,source.post_video_views_organic_unique,source.post_video_views_paid,source.post_video_views_paid_unique,source.post_video_views_unique,source.story,source.last_update );
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
        SET last_update = :current_date
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

#endregion

# Run the pages async script
def run_async_fetch_pages():
    # Run the async function in the event loop
    return asyncio.run(create_fact_facebook_accounts_daily_data())


# Run the posts async script
def run_async_fetch_posts():
    # Run the async function in the event loop
    return asyncio.run(create_fact_facebook_media_daily_data())



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
        dag_id="facebook_data_to_snowflake",
        default_args=default_args,
        description="Fetch Facebook accounts and media data and upload to Snowflake",
        schedule_interval="0 16 * * *",  # Run daily at 18:00 (israel time)
        start_date=datetime(2025, 1, 7),
        catchup=False,
) as dag:

    fetch_task_dim_accounts = PythonOperator(
        task_id="create_dim_facebook_accounts",
        python_callable=create_dim_facebook_accounts,
        on_failure_callback=send_slack_error_notification,
        #on_success_callback=send_slack_success_notification
    )

    fetch_task_fact_accounts = PythonOperator(
        task_id="create_fact_facebook_accounts_daily_data",
        python_callable=run_async_fetch_pages,
        on_failure_callback=send_slack_error_notification,
        #on_success_callback=send_slack_success_notification
    )

    fetch_task_fact_media = PythonOperator(
        task_id="create_fact_facebook_media_daily_data",
        python_callable=run_async_fetch_posts,
        on_failure_callback=send_slack_error_notification,
        #on_success_callback=send_slack_success_notification
    )

    save_task = PythonOperator(
        task_id="save_to_snowflake",
        python_callable=save_to_snowflake_facebook,
        on_failure_callback=send_slack_error_notification,
        on_success_callback=send_slack_success_notification
    )

    [fetch_task_dim_accounts, fetch_task_fact_accounts, fetch_task_fact_media] >> save_task