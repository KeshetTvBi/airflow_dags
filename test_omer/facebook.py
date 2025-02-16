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

# API endpoints
user_pages_url = f"https://graph.facebook.com/v22.0/me/accounts?fields=id,name,instagram_business_account&access_token={ACCESS_TOKEN}"
business_pages_url = f"https://graph.facebook.com/v22.0/{BUSINESS_ACCOUNT_ID}/owned_pages?fields=id,name,instagram_business_account&access_token={ACCESS_TOKEN}"

# Function to fetch Facebook accounts
def get_pages(source, url):
    response = requests.get(url).json()
    return [
        {
            "facebook_account_id": page["id"],
            "facebook_account_name": page["name"],
            "connected_instagram_account_id": page.get("instagram_business_account", {}).get("id"),
            "source": source
        }
        for page in response.get("data", [])
    ]

# Fetch and merge data
all_pages = get_pages("me/accounts", user_pages_url) + get_pages("business/owned_pages", business_pages_url)

# Convert to DataFrame
df_accounts = pd.DataFrame(all_pages)
df_accounts['last_update'] = datetime.now().date()

print(df_accounts)

# Optional: Save to CSV
df_accounts.to_csv(r"C:\Users\omer.yarchi\Desktop\Graph API for Facebook Insights\dim_facebook_accounts\dim_facebook_accounts.csv", index=False, encoding='utf-8-sig')
#endregion

#region fact_facebook_account_daily_data
######fact_facebook_account_daily_data

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

#API Endpoints
user_pages_url = f"https://graph.facebook.com/v22.0/me/accounts?fields=id,access_token&access_token={ACCESS_TOKEN}"
business_pages_url = f"https://graph.facebook.com/v22.0/{BUSINESS_ACCOUNT_ID}/owned_pages?fields=id,access_token&access_token={ACCESS_TOKEN}"


#Function to fetch data from the api endpoints
async def fetch_data(session, url):
    async with session.get(url) as response:
        response_json = await response.json()
        if "error" in response_json:
            logging.warning(f"API Error: {response_json['error']['message']}")
        return response_json

# Function to get Facebook Pages and their Access Tokens (we need the page access token to retrieve the page metrics, unlike instagram where we can use user token)
async def get_pages_with_tokens(session, url):
    response_json = await fetch_data(session, url) #Use rate-limited call

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
async def get_page_insights(session, page_id, page_token, start_date, end_date):
    logging.info(f"Fetching insights for Page ID: {page_id}")

    url = f"https://graph.facebook.com/v22.0/{page_id}/insights?metric={','.join(metrics)}&period=day&since={start_date}&until={end_date}&access_token={page_token}"

    response_json = await fetch_data(session, url)  #Use rate-limited call

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
                        "date": entry.get("end_time"),
                        "facebook_account_id": page_id,
                        metric_name: entry.get("value")
                    })

    return insights_list

# Main async function to handle all API calls concurrently
async def main_pages():
    async with aiohttp.ClientSession() as session:
        # Fetch pages from both endpoints concurrently
        user_pages_url = f"https://graph.facebook.com/v22.0/me/accounts?fields=id,access_token&access_token={ACCESS_TOKEN}"
        business_pages_url = f"https://graph.facebook.com/v22.0/{BUSINESS_ACCOUNT_ID}/owned_pages?fields=id,access_token&access_token={ACCESS_TOKEN}"

        user_pages = await get_pages_with_tokens(session, user_pages_url)
        business_pages = await get_pages_with_tokens(session, business_pages_url)

        all_pages = user_pages + business_pages  # Merge results

        # Set the date range. cant be more than 90 days
        start_date = "2025-02-01"  # Replace with your desired start date
        end_date = "2025-02-04"  # Replace with your desired end date

        # Fetch insights for all pages concurrently
        tasks = [
            get_page_insights(session, page["facebook_account_id"], page["page_access_token"], start_date, end_date)
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

        # Save to CSV
        df_pages.to_csv(
            r"C:\Users\omer.yarchi\Desktop\Graph API for Facebook Insights\fact_facebook_accounts_daily_data\fact_facebook_accounts_daily_data.csv",
            index=False, encoding='utf-8-sig')


# Run the async script
asyncio.run(main_pages())


#endregion

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
async def get_posts_for_page(session, page_id, page_token, start_date, end_date):
    url = f"https://graph.facebook.com/v22.0/{page_id}/feed?fields=id,created_time,message&since={start_date}&until={end_date}&access_token={page_token}"
    print(f"🔗 Requesting posts for page {page_id}: {url}")

    response_json = await fetch_data(session, url)  # ✅ Use rate-limited call

    if "error" in response_json:
        print(f"⚠️ API Error: {response_json['error']['message']}")
        return []

    if "data" not in response_json or not response_json["data"]:
        print(f"⚠️ No posts found for Page {page_id} within the date range {start_date} - {end_date}")
        return []


    posts=[]
    for post in response_json["data"]:
        posts.append({
            "post_id": post["id"],
            "post_title": post.get("message", ""),
            "facebook_account_id": page_id,
            "date_posted": post["created_time"],
            "page_access_token": page_token
        })


    return posts

#loop over post IDs and retrieve insights for each
async def get_post_insights(session, post_id, page_id,date_posted, start_date, end_date, page_token):
    fields_url = f"https://graph.facebook.com/v22.0/{post_id}?fields=shares.summary(true),likes.summary(true),comments.summary(true),reactions.summary(true),story,status_type,message&access_token={page_token}"
    fields_response = await fetch_data(session, fields_url)

    if "error" in fields_response :
        logging.error(f"⚠️ API Error: {fields_response ['error']['message']}")
        return []

    post_type = fields_response.get("status_type", "unknown")  # Check post type

    # Choose appropriate metrics based on post type
    metrics_to_fetch = post_metrics
    if post_type == "added_video":
        metrics_to_fetch = video_metrics  # Include video-specific metrics

    # Fetch insights
    insights_url = f"https://graph.facebook.com/v22.0/{post_id}/insights?metric={','.join(metrics_to_fetch)}&access_token={page_token}"
    insights_response = await fetch_data(session, insights_url)

    if "error" in insights_response :
        logging.error(f"⚠️ API Error: {insights_response ['error']['message']}")
        return []

    #insights = {"facebook_account_id": page_id, "post_id": post_id}
    insights_list = []
    if "data" in insights_response  and insights_response ["data"]:
        for metric_data in insights_response ["data"]:
            metric_name = metric_data["name"]
            if "values" in metric_data and metric_data["values"]:
                for entry in metric_data["values"]:
                    insights_list.append({
                    "date_posted": date_posted,
                    "facebook_account_id": page_id,
                    "post_id":post_id,
                    metric_name: entry.get("value")
                    })
                #insights[metric_name] = metric_data["values"][0]["value"]

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
    print(f"✅ Adding Post Info: {post_info}")
    insights_list.append(post_info)
    print(f"✅✅✅✅insight_list: {insights_list}")

    return insights_list #insights  # Dictionary with post metrics


#main
async def main_posts():
    async with aiohttp.ClientSession() as session:
        user_pages_url = f"https://graph.facebook.com/v22.0/me/accounts?fields=id,access_token&access_token={ACCESS_TOKEN}"
        business_pages_url = f"https://graph.facebook.com/v22.0/{BUSINESS_ACCOUNT_ID}/owned_pages?fields=id,access_token&access_token={ACCESS_TOKEN}"

        user_pages = await get_pages_with_tokens(session, user_pages_url)
        business_pages = await get_pages_with_tokens(session, business_pages_url)
        all_pages = user_pages + business_pages

        start_date = "2025-02-01"
        end_date = "2025-02-04"

        # 1️⃣ Fetch all posts for each page concurrently
        post_tasks = [get_posts_for_page(session, page["facebook_account_id"], page["page_access_token"], start_date, end_date) for page in all_pages]
        all_posts = await asyncio.gather(*post_tasks)

        # Flatten list of lists
        flattened_posts = [post for sublist in all_posts for post in sublist]
        # 2️⃣ Fetch insights for all posts concurrently
        post_insight_tasks = [
            get_post_insights(session, post["post_id"], post["facebook_account_id"], post["date_posted"],start_date, end_date, page["page_access_token"])
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
            print("\n📊 Checking DataFrame before pivoting:")
            print(df_posts.head(10))  # Check the first 10 rows
            print(df_posts.columns)  # Check column names
            #df_posts = df_posts.pivot_table(index=["date_posted", "facebook_account_id", "post_id"],
                                           # aggfunc="first").reset_index()
            df_posts["date_posted"].fillna(df_posts.groupby("post_id")["date_posted"].transform("first"), inplace=True)

            df_posts = df_posts.pivot_table(index=["date_posted", "facebook_account_id", "post_id"],
                                            aggfunc="first").reset_index()
            print(df_posts.columns)

        # Ensure all metrics exist (fill missing values with NaN)
        #df_posts = df_posts.pivot_table(index=["date_posted", "facebook_account_id", "post_id"], aggfunc="first").reset_index()

        # Add last_update column
        df_posts["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


        # Save to CSV
        df_posts.to_csv(r"C:\Users\omer.yarchi\Desktop\Graph API for Facebook Insights\fact_facebook_posts_daily_data\fact_facebook_posts_daily_data.csv", index=False, encoding='utf-8-sig')

# Run the async script
if __name__ == "__main__":
    asyncio.run(main_posts())

#endregion





