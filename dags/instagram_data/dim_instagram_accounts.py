# region dim_instagram_accounts with logging (fetching all facebook pages and their connected instagram account)

import logging

# Set up logging
logging.basicConfig(
    filename="C:/Users/omer.yarchi/Desktop/Graph API for Instagram Insights/dim_instagram_accounts/dim_instagram_accounts.log",
    # Log file name
    level=logging.INFO,  # Logging level
    format="%(asctime)s - %(levelname)s - %(message)s"
)

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
"USER" : "OmerY",
"PASSWORD" : "Oy!273g4",
"ACCOUNT" : "el66449.eu-central-1",
"WAREHOUSE" : "COMPUTE_WH",
"DATABASE" : "MAKO_DATA_LAKE",
"SCHEMA" : "PUBLIC"
}

# Log the start of the script
logging.info("Script started.")

# Initialize a dictionary to store unique pages (key = page_id)
unique_pages = {}

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
def fetch_pages(endpoint, params, source_name, unique_pages, **kwargs):
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
                if page_id not in unique_pages:
                    unique_pages[page_id] = {
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
        logging.error(f"Exception in fetch_pages: {e}")


# Main function to create the dim_instagram_accounts table
def create_dim_instagram_accounts(**kwargs):
    try:
        logging.info("Script started successfully.")
        # Initialize a dictionary to store unique pages (key = page_id)
        unique_pages = {}
        # print("Script is running")
        # Step 1: Fetch Pages from /owned_pages
        logging.info("Fetching Pages from Owned Pages Endpoint")
        owned_pages_url = f"https://graph.facebook.com/v21.0/{BUSINESS_ACCOUNT_ID}/owned_pages"
        params = {
            "fields": "id,name,instagram_business_account",
            "access_token": ACCESS_TOKEN
        }
        fetch_pages(owned_pages_url, params, "owned_pages")

        # Step 2: Fetch Pages from /me/accounts
        logging.info("Fetching Pages from Me Accounts Endpoint")
        accounts_url = "https://graph.facebook.com/v21.0/me/accounts"
        fetch_pages(accounts_url, params, "me/accounts")

        # Step 3: Fetch Pages from /client_pages
        logging.info("Fetching Pages from Client Pages Endpoint")
        client_pages_url = f"https://graph.facebook.com/v21.0/{BUSINESS_ACCOUNT_ID}/client_pages"
        fetch_pages(client_pages_url, params, "client_pages")

        # Save to CSV
        current_date = datetime.now().strftime("%d_%m_%y")
        df_pages = pd.DataFrame.from_dict(unique_pages, orient="index")
        df_pages.dropna(subset=["instagram_account_id"], inplace=True)
        df_pages['last_update'] = datetime.now().date()
        try:
            desktop_path = f"C:/Users/omer.yarchi/Desktop/Graph API for Instagram Insights/dim_instagram_accounts/dim_instagram_accounts_{current_date}.csv"
            df_pages.to_csv(
                desktop_path, index=False, encoding='utf-8-sig')
            logging.info(f"Data saved to {desktop_path} successfully")
        except Exception as e:
            logging.error(f"Error saving csv: {e}")

        return unique_pages  # Return the dictionary
    except Exception as e:
        logging.error(f"Error: {e}")        # upload data to snowflake table mako_data_lake.public.dim_instagram_account
        raise

        # from sqlalchemy import create_engine
        # from sqlalchemy.orm import sessionmaker
        # from sqlalchemy.sql import text
def save_to_snowflake(ti, **kwargs):
    table = "dim_instagram_accounts"
    table_staging = "dim_instagram_accounts_staging"

    # Pull `unique_pages` from XCom
    unique_pages = ti.xcom_pull(task_ids="create_dim_instagram_accounts")
    if not unique_pages:
        logging.error("No data found in XCom for `unique_pages`.")
        raise ValueError("No data found in XCom.")

    # Step 4: Convert Results to a DataFrame
    df_pages = pd.DataFrame.from_dict(unique_pages, orient="index")
    df_pages.dropna(subset=["instagram_account_id"], inplace=True)
    df_pages['last_update'] = datetime.now().date()
    logging.info(f"Combined Pages and Instagram Accounts. Total records: {len(df_pages)}")

    # Connect to Snowflake using SQLAlchemy
    engine = create_engine(
        f"snowflake://{SNOWFLAKE_CONFIG['user']}:{SNOWFLAKE_CONFIG['password']}@{SNOWFLAKE_CONFIG['account']}/"
        f"{SNOWFLAKE_CONFIG['database']}/{SNOWFLAKE_CONFIG['schema']}?warehouse={SNOWFLAKE_CONFIG['warehouse']}"
    )
    # Upload data to dim_instagram_accounts_staging
    try:
        df_pages.to_sql(
            f"{table_staging}",
            # this staging table is being created when executing this. Its being dropped after the upsert
            con=engine,
            index=False,
            if_exists="replace",
            method="multi",
        )
        logging.info(f"Data uploaded successfully to {table_staging}")
    except Exception as e:
        logging.error(f"Error uploading data to staging: {e}")

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
        logging.info("Upsert operation completed successfully.")
    except Exception as e:
        logging.error(f"Error during upsert: {e}")
        session.rollback()
    finally:
        session.close()

    # Query to count rows in the dim after upsert
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

    # Drop the staging table
    try:

        cleanup_query = text(f"DROP TABLE IF EXISTS {SNOWFLAKE_CONFIG['DATABASE']}.{SNOWFLAKE_CONFIG['SCHEMA']}.{table_staging}")
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
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=3)
}

with DAG(
    dag_id="dim_instagram_accounts_dag",
    default_args=default_args,
    description="Fetch Instagram accounts data and upload to Snowflake",
    schedule_interval="0 17 * * *",  # Run daily at 17:00
    start_date=datetime(2024, 1, 7),
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id="create_dim_instagram_accounts",
        python_callable=create_dim_instagram_accounts,
    )

    save_task = PythonOperator(
        task_id="save_to_snowflake",
        python_callable=save_to_snowflake,
    )

    fetch_task >> save_task