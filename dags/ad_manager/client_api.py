from googleads import ad_manager
import time
import logging
import gzip
import tempfile
import csv

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class Client:
    def __init__(self, client):
        self.client = client

    def download_report(self, saved_query_id, start_date, end_date):
        log.info(f'Define report job')
        report_service = self.client.GetService('ReportService', version='v202402')

        statement = (ad_manager.StatementBuilder(version='v202402')
                     .Where('id = :id')
                     .WithBindVariable('id', int(saved_query_id))
                     .Limit(1))

        response = report_service.getSavedQueriesByStatement(statement.ToStatement())
        log.info(f'Response: {response}')

        report_job = {}
        if response and 'results' in response:
            saved_query = response['results'][0]

            log.info(f'Specify the report query')
            report_job['reportQuery'] = saved_query['reportQuery']
            report_job['reportQuery']['startDate'] = start_date
            report_job['reportQuery']['endDate'] = end_date

        else:
            log.error(f'No saved queries found or the response is empty')

        log.info(f'Run the report')

        report_job = report_service.runReportJob(report_job)
        report_job_id = report_job['id']
        log.info(f'Report Job ID: {report_job_id}')

        # Wait for the report to complete
        status = report_service.getReportJobStatus(report_job_id)
        while status != 'COMPLETED':
            print(f'Report status: {status}')
            time.sleep(30)  # Wait before checking again
            status = report_service.getReportJobStatus(report_job_id)

        log.info(f'Report completed!')

        log.info(f'Download the report')

        export_format = 'CSV_DUMP'
        report_downloader = self.client.GetDataDownloader(version='v202402')
        compressed_file = tempfile.NamedTemporaryFile(dir='dags/ad_manager/tmp', suffix='.csv.gz', delete=False)

        report_downloader.DownloadReportToFile(report_job_id, export_format, compressed_file)

        with gzip.open(compressed_file.name, 'rt', newline='', encoding='utf8') as f_in:
            dfp_report_data = list(csv.DictReader(f_in))

        return dfp_report_data

