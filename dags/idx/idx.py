import os
import shutil
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import pendulum
import requests
from dateutil.rrule import rrule, MONTHLY
import csv
import datetime
from pydomo import Domo
from pydomo.datasets import UpdateMethod
import io
from airflow.models import Variable
import pandas as pd
import json
import logging

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

domo = Domo(Variable.get('domoKey'), Variable.get('domoSecret'), api_host='api.domo.com')
dataset_id = '057533ab-4a43-41d2-a0cd-0286d52f401b'


default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2023, 9, 4, tz='Asia/Jerusalem'),
    'email': ['tomer.man@mako.co.il'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=3),
    'provide_context': True
}


def send_slack_error_notification(context):
    slack_token = '****'
    slack_channel = 'C05UNMWHX2R'

    message = f"An error occurred in the Airflow DAG '{context['dag'].dag_id}' on {context['execution_date']}.\nError Message: {context['exception']}"

    try:
        client = WebClient(token=slack_token)
        response = client.chat_postMessage(channel=slack_channel, text=message)
        assert response['message']['text'] == message
    except SlackApiError as e:
        log.info(f"Error sending Slack message: {e.response['error']}")


# helper to format dates
def parseDateIfItsADate(v):
    try:
        return datetime.datetime.strptime(v, '%d/%m/%Y').strftime('%Y-%m-%d')
    except:
        return v


def CCase(s):
    new_s = ''
    skip_letter = False
    for i in range(len(s)):

        if s[i] == "_" and i < len(s) - 1:
            new_s = new_s + s[i + 1].upper()
            skip_letter = True
        elif not skip_letter:
            new_s = new_s + s[i]
        else:
            skip_letter = False
    return new_s


def selfJoinPrices(a, b):
    return a['Buyer'] + a['Brand'] + a['Product'] == b['Buyer'] + b['Brand'] + b['Product']


def extract_price(**kwargs):
    now_dt = datetime.datetime.now()
    today_dt = datetime.datetime(now_dt.year, now_dt.month, now_dt.day)

    log.info(f'Extract data from price file')
    try:
        with open('dags/idx/prices.csv', 'r', encoding='utf-8-sig') as f:
            pricelist = [{**x, **{'stringDate': x['Date']}} for x in csv.DictReader(f)]

    except FileNotFoundError as e:
        log.error(e)
    except Exception as e:
        log.error(f'An error occurred: {e}')


    for row in pricelist:
        row['Date'] = datetime.datetime.strptime(row['Date'], '%d/%m/%Y')
        row['EndDate'] = today_dt
        distinct_dates = [datetime.datetime.strptime(d, '%d/%m/%Y') for d in
                          {x['stringDate'] for x in pricelist if selfJoinPrices(x, row)}]
        greater_dates = [x for x in distinct_dates if x > row['Date']]

        if len(greater_dates) > 0:
            row['EndDate'] = greater_dates[0] - datetime.timedelta(days=1)

    kwargs['ti'].xcom_push(key='idx_pricelist', value=pricelist)


def domo_schema(**kwargs):
    log.info(f'Get schema from Domo')
    try:
        target_schema = [x['name'] for x in domo.datasets.get(dataset_id)['schema']['columns']]

    except KeyError as e:
        log.error(f'Key error occurred: {e}')
    except domo.DomoError as e:
        log.error(f'Error retrieving dataset from Domo: {e}')
    except Exception as e:
        log.error(f'An unexpected error occurred: {e}')

    fields_order_first = [CCase(x) for x in target_schema if
                          x not in ['buyer', 'calculated_cost', 'price_after_discount', 'price_after_commission',
                                    'impressions', 'create_date', 'clicks', 'cost', 'full_view', 'netto_price']]
    fields_order = [x if x in ['buyer', 'calculated_cost', 'price_after_discount', 'price_after_commission',
                               'netto_price'] else CCase(x) for x in target_schema]

    kwargs['ti'].xcom_push(key='idx_fields_order_first', value=fields_order_first)
    kwargs['ti'].xcom_push(key='idx_fields_order', value=fields_order)


def request_idx(**kwargs):
    log.info(f'{datetime.datetime.now()} --> start import')

    # get one year ago
    from_date = datetime.datetime.today() - datetime.timedelta(days=60)
    # from_date = datetime.datetime(2024,1,1)
    log.info(f'{datetime.datetime.now()} --> importing active campaigns')

    try:
        url = 'https://terminal.id-x.co.il/rest/api/v1/integration/app/report/campaigns/active'
        log.info(f'Attempting to fetch data from: {url}')

        active_req = requests.get(url, headers={'X-AUTH-TOKEN': Variable.get('idx_key')})
        active = active_req.json()

    except requests.exceptions.HTTPError as http_err:
        log.error(f'HTTP error occurred: {http_err}')
    except requests.exceptions.Timeout as timeout_err:
        log.error(f'Timeout error occurred: {timeout_err}')

    archived = []

    # take all archived campains for each month since the Jan 1st last year
    first_of_month_dates = [dt for dt in
                            rrule(MONTHLY, dtstart=from_date, until=datetime.datetime.today(), bymonthday=1)]
    log.info(f'{datetime.datetime.now()} --> importing archived campaigns')
    for m in first_of_month_dates:
        obj = None
        errcount = 0
        while obj is None:
            try:
                log.info(f"{datetime.datetime.now()} --> importing {m.strftime('%B %Y')}")
                from_date = (m - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                to_date = (datetime.datetime(m.year if m.month < 12 else m.year + 1, m.month + 1 if m.month < 12 else 1,
                                             1) - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                url = f'https://terminal.id-x.co.il/rest/api/v1/integration/app/report/campaigns/archived?startDate={from_date}&endDate={to_date}'
                log.info(f'Attempting to fetch data from: {url}')

                try:
                    obj = requests.get(url, headers={'X-AUTH-TOKEN': Variable.get('idx_key')})

                except requests.exceptions.HTTPError as http_err:
                    log.error(f'HTTP error occurred: {http_err}')
                except requests.exceptions.Timeout as timeout_err:
                    log.error(f'Timeout error occurred: {timeout_err}')

                archived = archived + obj.json()
            except Exception as e:
                try:
                    log.info(f'Error ---> {obj.text}')
                except:
                    log.info(f'Error ---> {e}')
                errcount = errcount + 1
                if errcount > 10:
                    continue

    with open('dags/idx/tmp/active.json', 'w') as json_file:
        json.dump(active, json_file, indent=4)

    with open('dags/idx/tmp/archived.json', 'w') as json_file:
        json.dump(archived, json_file, indent=4)


def transform_data(**kwargs):
    pricelist = kwargs['ti'].xcom_pull(task_ids='extract_price', key='idx_pricelist')
    fields_order_first = kwargs['ti'].xcom_pull(task_ids='domo_schema', key='idx_fields_order_first')

    with open('dags/idx/tmp/active.json', 'r') as json_file:
        active = json.load(json_file)

    with open('dags/idx/tmp/archived.json', 'r') as json_file:
        archived = json.load(json_file)


    final_dat = []

    # get the required keys
    flat_keys = [
        key for key in active[0].keys()
        if str(type(active[0][key])) not in ["<class 'dict'>", "<class 'list'>"]
        if key not in ['cost', 'impressions', 'clicks', 'uniqueUsers']
    ]
    if 'videoStatistics' not in flat_keys:
        flat_keys.append('videoStatistics')

    log.info(f'{datetime.datetime.now()} --> extracting daily data')

    # flatten the daily data
    for row in active:
        discount_value = None
        discount_type = None
        compensation = False
        netto_price = None
        if 'distributorStatistics' in row.keys() and len(row['distributorStatistics']) > 0:
            netto_price = row['distributorStatistics'][0]['price']
        if 'discountType' in row.keys() and 'discountValue' in row.keys():
            discount_type = row['discountType']
            discount_value = row['discountValue']
        if 'compensation' in row.keys():
            compensation = row['compensation']
        row = {**{k: (row[k] if k in row.keys() else '') for k in fields_order_first},
               **{'company': row['company'], 'detailedStatistics': row['detailedStatistics'],
                  'netto_price': netto_price}}
        base_obj = {**{key: parseDateIfItsADate(row[key]) for key in fields_order_first},
                    **{'buyer': row['company']['name'], 'netto_price': row['netto_price'],
                       'discount_value': discount_value, 'discount_type': discount_type, 'compensation': compensation}}
        dated_objs = [{**{key: parseDateIfItsADate(d[key]) for key in d.keys()}, **base_obj} for d in
                      row['detailedStatistics']]
        final_dat = final_dat + dated_objs

    for row in archived:
        discount_value = None
        discount_type = None
        compensation = False
        netto_price = None
        if 'distributorStatistics' in row.keys() and len(row['distributorStatistics']) > 0:
            try:
                netto_price = row['distributorStatistics'][0]['price']
            except KeyError:
                pass
        if 'discountType' in row.keys() and 'discountValue' in row.keys():
            discount_type = row['discountType']
            discount_value = row['discountValue']
        if 'compensation' in row.keys():
            compensation = row['compensation']
        row = {**{k: (row[k] if k in row.keys() else '') for k in fields_order_first},
               **{'company': row['company'], 'detailedStatistics': row['detailedStatistics'],
                  'netto_price': netto_price}}
        base_obj = {**{key: parseDateIfItsADate(row[key]) for key in fields_order_first},
                    **{'buyer': row['company']['name'], 'netto_price': row['netto_price'],
                       'discount_value': discount_value,
                       'discount_type': discount_type, 'compensation': compensation}}
        dated_objs = [{**{key: parseDateIfItsADate(d[key]) for key in d.keys()}, **base_obj} for d in
                      row['detailedStatistics']]
        final_dat = final_dat + dated_objs

    dupcheck = []
    final_dat_no_dups = []

    for row in final_dat:

        key = ''.join([str(x) for x in row.values()])
        if key not in dupcheck:
            rowDate = datetime.datetime.strptime(row['createDate'], '%Y-%m-%d')

            priceJoinObj = {''.join([x['Buyer'], x['Brand'], x['Product']]): {
                'Price': float(x['Price'].replace('₪', '').replace('-', '0').strip()),
                'Commission': float(x['Commission'])} for x in pricelist if
                x['Date'].replace(tzinfo=None) <= rowDate.replace(tzinfo=None) <= x['EndDate'].replace(tzinfo=None)}

            dupcheck.append(key)
            # try join here
            if row['compensation']:
                row['netto_price'] = 32

            if row['discount_type'] == 'INVOICE':
                row['netto_price'] = row['netto_price'] - (row['netto_price'] * (row['discount_value'] / 100))

            if row['netto_price'] is None:
                row['netto_price'] = 0

            else:
                row['price_after_discount'] = row['impressions'] / 1000 * (row['blockCost'] + row['dataCost'])
                row['price_after_commission'] = row['price_after_discount'] - row['price_after_discount'] * 0.25
                row['direct_or_programmatic'] = 'direct'

            try:
                try:
                    row['price_after_discount'] = row['impressions'] / 1000 * priceJoinObj[
                        ''.join([row['buyer'], row['brandName'], row['blockType']])]['Price']
                    row['price_after_commission'] = row['price_after_discount'] - row['price_after_discount'] * \
                                                    priceJoinObj[
                                                        ''.join([row['buyer'], row['brandName'], row['blockType']])][
                                                        'Commission']
                except:
                    row['price_after_discount'] = row['impressions'] / 1000 * \
                                                  priceJoinObj[''.join([row['buyer'], row['blockType']])]['Price']
                    row['price_after_commission'] = row['price_after_discount'] - row['price_after_discount'] * \
                                                    priceJoinObj[''.join([row['buyer'], row['blockType']])][
                                                        'Commission']

            except:

                if row['buyer'] == 'Benefit' and datetime.datetime.strptime(row['createDate'],
                                                                            '%Y-%m-%d') < datetime.datetime.strptime(
                    '2023-08-13', '%Y-%m-%d'):
                    base_price = row['impressions'] / 1000 * (row['blockCost'] + row['dataCost'])
                    row['price_after_discount'] = base_price - base_price * 0.07
                    row['price_after_commission'] = row['price_after_discount']
                    row['direct_or_programmatic'] = 'direct'

                else:
                    row['price_after_discount'] = row['impressions'] / 1000 * (row['blockCost'] + row['dataCost'])
                    row['price_after_commission'] = row['price_after_discount'] - row['price_after_discount'] * 0.25
                    row['direct_or_programmatic'] = 'direct'

            row['calculated_cost'] = row['impressions'] / 1000 * (row['blockCost'] + row['dataCost'])
            final_dat_no_dups.append(row)
        else:
            continue

    df_final = pd.DataFrame(final_dat_no_dups)
    df_final.to_csv('dags/idx/tmp/final.csv', index=False)


def push_to_domo(**kwargs):
    df_final = pd.read_csv('dags/idx/tmp/final.csv')
    final_dat_no_dups = df_final.to_dict(orient='records')

    fields_order = kwargs['ti'].xcom_pull(task_ids='domo_schema', key='idx_fields_order')

    log.info(f'{datetime.datetime.now()} --> push to domo')

    templist = []
    # fields_order = final_dat_no_dups[0].keys()
    for row in final_dat_no_dups:

        if not row['blockType'].startswith('MAKO_'):
            row['direct_or_programmatic'] = 'programmatic'
            row['price_after_discount'] = row['impressions'] / 1000 * (row['blockCost'] + row['dataCost'])
            row['price_after_commission'] = row['impressions'] / 1000 * row['netto_price']
        templist.append({k: row[k] for k in fields_order if k != 'duration'})

    final_dat_no_dups = templist

    s = io.StringIO()
    write = csv.DictWriter(s, fieldnames=final_dat_no_dups[0].keys())
    write.writerows(final_dat_no_dups)

    log.info(f'Push dataset to Domo')
    try:
        domo.datasets.data_import(dataset_id, s.getvalue().strip(), UpdateMethod.APPEND)
    except KeyError as e:
        log.error(f'Key error occurred: {e}')
    except domo.DomoError as e:
        log.error(f'Error retrieving dataset from Domo: {e}')
    except Exception as e:
        log.error(f'An unexpected error occurred: {e}')


def cleanup_files():
    log.info(f'Remove all files from the temporary directory')
    tmp_path = 'dags/idx/tmp'
    if os.path.exists(tmp_path):
        shutil.rmtree(tmp_path)  # Remove the directory and all its contents
        os.makedirs(tmp_path)  # Recreate the directory

    else:
        log.error(f'Directory {tmp_path} does not exist')


with DAG(dag_id='idx_v2',
         # schedule_interval='50 3 * * *',
         schedule_interval=None,
         catchup=False,
         default_args=default_args) as dag:

    extract_price_task = PythonOperator(
        task_id='extract_price',
        python_callable=extract_price,
        provide_context=True,
        on_failure_callback=send_slack_error_notification
    )

    domo_schema_task = PythonOperator(
        task_id='domo_schema',
        python_callable=domo_schema,
        provide_context=True,
        on_failure_callback=send_slack_error_notification
    )

    request_idx_task = PythonOperator(
        task_id='request_idx',
        python_callable=request_idx,
        provide_context=True,
        on_failure_callback=send_slack_error_notification
    )

    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
        on_failure_callback=send_slack_error_notification
    )

    push_to_domo_task = PythonOperator(
        task_id='push_to_domo',
        python_callable=push_to_domo,
        provide_context=True,
        on_failure_callback=send_slack_error_notification
    )

    cleanup_files_task = PythonOperator(
        task_id='cleanup_files',
        python_callable=cleanup_files,
    )

    [extract_price_task, domo_schema_task, request_idx_task] >> transform_data_task >> push_to_domo_task >> cleanup_files_task
