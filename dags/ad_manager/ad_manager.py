from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import traceback
import pendulum
import datetime
from datetime import timedelta
import os
import tempfile
from googleads import ad_manager
from googleads import errors
import _locale
import gzip
import csv
import io
from pydomo import Domo
# import credentials
# import domoJavaCLIConnector


default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2023, 9, 4, tz='Asia/Jerusalem'),
    'email': ['tal.ugashi@keshet-d.co.il'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=3),
    'provide_context': True
}

def main():
    print(f'{datetime.datetime.now()} ---> DFP creative report')

    domo = Domo(Variable.get('domoKey'), Variable.get('domoSecret'), api_host='api.domo.com')

    dfp_dataset_id = '4e35d12f-8061-4d3b-b726-efc7728f603c'
    fields = [x['name'] for x in domo.datasets.get(dfp_dataset_id)['schema']['columns']]

    now = datetime.datetime.today()
    from_date = now - datetime.timedelta(days=30)
    # from_date = datetime.datetime(2022,1,1)
    # now = datetime.datetime(2022,12,30)

    weeks = [(from_date + datetime.timedelta(days=i), from_date + datetime.timedelta(days=i + 6)) for i in
             range(0, (now - from_date).days, 7)]
    weeks.reverse()
    dfp_report_data_final = []
    for f_date, t_date in weeks:
        if t_date > datetime.datetime.today():
            t_date = datetime.datetime.today()
        nowObj = {
            'year': t_date.year,
            'month': t_date.month,
            'day': t_date.day
        }
        weekagoObj = {
            'year': f_date.year,
            'month': f_date.month,
            'day': f_date.day
        }

        print(f'{datetime.datetime.now()} ---> get DFP report for {f_date.date()} - {t_date.date()}')
        _locale._getdefaultlocale = (lambda *args: ['en_US', 'UTF-8'])
        saved_query_id = 13653086304
        client = ad_manager.AdManagerClient.LoadFromStorage('dags/ad_manager/googleads.yaml')
        report_service = client.GetService('ReportService', version='')
        statement = (ad_manager.StatementBuilder(version='')
                     .Where('id = :id')
                     .WithBindVariable('id', int(saved_query_id))
                     .Limit(1))
        report_downloader = client.GetDataDownloader(version='')
        response = report_service.getSavedQueriesByStatement(statement.ToStatement())

        if 'results' in response and len(response['results']):

            saved_query = response['results'][0]

            report_job = {}
            report_job['reportQuery'] = saved_query['reportQuery']
            report_job['reportQuery']['startDate'] = weekagoObj
            report_job['reportQuery']['endDate'] = nowObj
            report_job_id = report_downloader.WaitForReport(report_job)

            try:

                report_job_id = report_downloader.WaitForReport(report_job)

            except errors.AdManagerReportError as e:
                print('Failed to generate report. Error was: %s' % e)
            export_format = 'CSV_DUMP'

            report_file = tempfile.NamedTemporaryFile(suffix='.csv.gz', delete=False)

            report_downloader.DownloadReportToFile(report_job_id, export_format, report_file)
            print(f'{datetime.datetime.now()} ---> raw report temp file - {report_file.name}')
            report_file.close()

            if 'results' in response and len(response['results']):
                print(f'{datetime.datetime.now()} ---> getting report data')
                with gzip.open(report_file.name, 'rt', newline='', encoding="utf8") as f_in:
                    dfp_report_data = list(csv.DictReader(f_in))
                os.remove(report_file.name)
        print(f'{datetime.datetime.now()} ---> fixing column names')
        dfp_report_data_final = dfp_report_data_final + [
            {key.replace('Dimension.', '').replace('DimensionAttribute.', '').replace('Column.', '').lower(): (
                value if value != 'N/A' else None) for
             key, value in row.items()} for row in dfp_report_data]

    dfp_report_data_final = [{k: x[k] for k in fields} for x in dfp_report_data_final]
    print(f'{datetime.datetime.now()} ---> push report data to domo')
    csvMem = io.StringIO()
    write = csv.DictWriter(csvMem, fieldnames=dfp_report_data_final[0].keys())
    # write.writeheader()
    write.writerows(dfp_report_data_final)
    fpath = f'dfp_{f_date.date()}-{t_date.date()}.csv'
    with open(fpath, 'w', encoding='utf-8', newline='') as tf:
        tf.write(csvMem.getvalue())
    # domoJavaCLIConnector.pushData(file_path=os.getcwd() + '\\' + fpath, dataset_id=dfp_dataset_id, append=True)
    # os.remove(fpath)
    # domo.datasets.data_import(dfp_dataset_id, csvMem.getvalue(), UpdateMethod.APPEND)

    '''
    print(f'{datetime.datetime.now()} ---> push idx translator data to domo')
    csvMem2 = io.StringIO()
    write = csv.DictWriter(csvMem2, fieldnames=sheet_dataset_list[0].keys())
    write.writerows(sheet_dataset_list)

    domo.datasets.data_import(idx_translator_dataset_id, csvMem2.getvalue(), UpdateMethod.REPLACE)
    print(f'{datetime.datetime.now()} ---> done')
    '''


try:
    main()
except Exception as e:
    print(traceback.format_exc())
    # slackErrorAlert(str(e), traceback.format_exc(), __file__.split("\\")[-1])

with DAG(
        'ad_manager',
        default_args=default_args,
        schedule_interval=None,
        catchup=False
) as dag:

    read_file_task = PythonOperator(
        task_id='read_file',
        python_callable=main(),
        provide_context=True,
        # on_failure_callback=send_slack_error_notification
    )

    read_file_task