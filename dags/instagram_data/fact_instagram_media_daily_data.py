
# region fact_instagram_media_daily_data with logging (fetching all facebook pages and their connected instagram account)
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
    import asyncio
    import aiohttp
    import pandas as pd
    #import sys
    from datetime import datetime,timedelta
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
#access token expire on february 15 2025. to extend go here https://developers.facebook.com/tools/debug/accesstoken/
BUSINESS_ACCOUNT_ID = "10154132454588797"  # The Facebook Business Account ID
#PAGE_ID = "100462013796" #mako#       # Facebook Page ID connected to Instagram Business Account
SNOWFLAKE_CONFIG = {
# Snowflake connection parameters
"user" : "OmerY",
"password" : "Oy!273g4",
"account" : "el66449.eu-central-1",
"warehouse" : "COMPUTE_WH",
"database" : "MAKO_DATA_LAKE",
"schema" : "PUBLIC"
}

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
    "total_interactions"
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
    "total_interactions"
]

STORY_METRICS = [
    "impressions",
    "reach",
    "replies",
    "shares",
    "total_interactions"
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

def save_to_snowflake(ti, **kwargs):

    table = "fact_instagram_media_daily_data"
    table_staging = "fact_instagram_media_daily_data_staging"

    try:
        # Pull `all_records` from XCom
        all_records = ti.xcom_pull(task_ids="create_fact_instagram_media_daily_data")
        if not all_records:
            logging.error("No data found in XCom for `all_records`.")
            raise ValueError("No data found in XCom.")

        # Convert Results to a DataFrame
        df_media_metrics = pd.DataFrame(all_records)
        # Debugging logs
        logging.info(f"df_media_metrics columns: {df_media_metrics.columns}")
        logging.info(f"df_media_metrics head: {df_media_metrics.head()}")

        if "instagram_account_id" not in df_media_metrics.columns:
            logging.error("The column 'instagram_account_id' is missing in the DataFrame.")
            return
        df_media_metrics['last_update'] = datetime.now().date()  # adding current date as the table's last update date
        logging.info("Combined Media Metrics Data")
    except Exception as e:
        logging.error(f"Error combining media metrics data: {e}")

    # Connect to Snowflake using SQLAlchemy
    engine = create_engine(
        f"snowflake://{SNOWFLAKE_CONFIG['user']}:{SNOWFLAKE_CONFIG['password']}@{SNOWFLAKE_CONFIG['account']}/"
        f"{SNOWFLAKE_CONFIG['database']}/{SNOWFLAKE_CONFIG['schema']}?warehouse={SNOWFLAKE_CONFIG['warehouse']}"
    )

    # Upload data to fact_instagram_accounts_daily_data_staging
    try:
        # If the table doesn't exist, create it; otherwise, append data
        df_media_metrics.to_sql(
            f"{table_staging}",
            con=engine,
            index=False,  # Do not write the DataFrame index as a separate column
            if_exists="replace",  # Options: 'fail', 'replace', 'append'
            method="multi",  # Optimized bulk insert
        )
        logging.info(f"Data uploaded successfully to {table_staging}")
    except Exception as e:
        logging.error(f"Error uploading data: {e}")

    try:
        Session = sessionmaker(bind=engine)
        session = Session()
        # rows in the staging table
        count_new_rows = text(f"""select count(*) as row_count from {table_staging}""")
        result = session.execute(count_new_rows)
        row_count = result.scalar()
        logging.info(f"Number of rows inserted to {table_staging}: {row_count}")
        # rows in fact before upsert
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
            last_update = source.last_update
    WHEN NOT MATCHED THEN
        INSERT (instagram_account_id, media_id, date_posted, title, media_type, permalink, thumbnail_url, impressions, reach, saved, comments, likes, profile_activity, profile_visits, shares, total_interactions, plays, ig_reels_aggregated_all_plays_count, ig_reels_avg_watch_time, ig_reels_video_view_total_time, last_update)
        VALUES (source.instagram_account_id, source.media_id, source.date_posted, source.title, source.media_type, source.permalink, source.thumbnail_url, source.impressions, source.reach, source.saved, source.comments, source.likes, source.profile_activity, source.profile_visits, source.shares, source.total_interactions, source.plays, source.ig_reels_aggregated_all_plays_count, source.ig_reels_avg_watch_time, source.ig_reels_video_view_total_time, source.last_update);
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
    dag_id="fact_instagram_media_daily_data",
    default_args=default_args,
    description="Fetch Instagram media daily metrics data and upload to Snowflake",
    schedule_interval="0 15 * * *",  # Run daily at 17:00
    start_date=datetime(2025, 1, 7),
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id="create_fact_instagram_media_daily_data",
        python_callable=create_fact_instagram_media_daily_data,
    )

    save_task = PythonOperator(
        task_id="save_to_snowflake",
        python_callable=save_to_snowflake,
    )

    fetch_task >> save_task






























