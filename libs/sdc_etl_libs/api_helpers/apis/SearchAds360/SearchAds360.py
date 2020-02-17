
import json
import logging
import time
import pandas as pd
from io import StringIO
from sdc_etl_libs.api_helpers.API import API
from sdc_etl_libs.sdc_dataframe.Dataframe import Dataframe, SDCDFTypes
from sdc_etl_libs.sdc_file_helpers.SDCFileHelpers import SDCFileHelpers

logging.basicConfig(level=logging.INFO)


class SearchAds360(API):

    def __init__(self, credential_type_="aws_secrets"):

        self.credentials = self.get_credentials(credential_type_, "SearchAds360/api")
        self.credentials["private_key"] = self.credentials["private_key"].replace('\\n', '\n')
        self.service = self.get_google_service_credentials(
            service_name_='doubleclicksearch', version_='v2', certificate_=self.credentials,
            scopes_=['https://www.googleapis.com/auth/doubleclicksearch'])

        self.agencyId = "20700000001263908"
        self.statisticsCurrency = "usd"
        self.statisticsTimeZone = 'UTC'
        self.filters = None
        self.schemaName = None
        self.advertiserId = None
        self.engineAccountId = None
        self.startDate = None
        self.endDate = None
        self.startRow = None
        self.rowCount = None
        self.reportType = None
        self.columns = None
        self.changedAttributesSinceTimestamp = None
        self.changedMetricsSinceTimestamp = None

    def __request_report(self, body_=None):
        """
        Generate the body for the report request and makes the request to Google.
        :param body_: Dict. Complete body message to make report request with. If None, body will be constructed from
            params provided from Class attributes.
        :return: String. Report ID returned by Google.
        """

        if body_:
            body = body_
        else:
            body = {
                    "reportScope": {
                        "agencyId": self.agencyId,
                        "advertiserId": self.advertiserId
                    },
                    "reportType": self.reportType,
                    "columns": [],
                    "downloadFormat": "csv",
                    "maxRowsPerFile": 1000000,
                    "statisticsCurrency": self.statisticsCurrency,
                    "statisticsTimeZone": self.statisticsTimeZone
                }

            for column in self.columns:
                body["columns"].append(
                    {"columnName": column}
                )

            if self.startDate or self.endDate \
                    or self.changedAttributesSinceTimestamp or self.changedMetricsSinceTimestamp:
                body["timeRange"] = {}
                if self.startDate:
                    body["timeRange"]["startDate"] = self.startDate
                if self.endDate:
                    body["timeRange"]["endDate"] = self.endDate
                if self.changedAttributesSinceTimestamp:
                    body["timeRange"]["changedAttributesSinceTimestamp"] = self.changedAttributesSinceTimestamp
                if self.changedMetricsSinceTimestamp:
                    body["timeRange"]["changedMetricsSinceTimestamp"] = self.changedMetricsSinceTimestamp

            if self.filters is not None:
                body["filters"] = self.filters

            if self.advertiserId is not None:
                body["reportScope"]["advertiserId"] = self.advertiserId
                body["reportScope"]["engineAccountId"] = self.engineAccountId

        logging.info(f"Params being sent to the report request: {body}")

        request = self.service.reports().request(body=body)
        feedback = request.execute()
        report_id = feedback["id"]
        logging.info(f"Report ID: {report_id}")
        return report_id

    def __get_report_status(self, report_id_):
        """
        Makes request for report status to check if report is ready to download.
        :param report_id_: String. Report ID from Google.
        :return: Dict. Results of file status check. Example return value:

        {
          'isReportReady': True,
          'files': [
            {
              'url': 'https://www.googleapis.com/doubleclicksearch/v2/reports/AAAddreaEEx/files/0',
              'byteCount': '560929'
            }
          ]
        }
        """

        request = self.service.reports().get(reportId=report_id_)
        report_check_info = request.execute()
        ping = dict()
        ping["isReportReady"] = report_check_info["isReportReady"]
        if ping["isReportReady"] is True:
            ping["files"] = report_check_info["files"]
        else:
            ping["files"] = None
        logging.info(f"Report status: {ping}")
        return ping

    def __fetch_report(self, report_id_, report_fragment_):
        """
        Fetches report and data.
        :param report_id_: String. Report ID from Google.
        :param report_fragment_: Int. The index of the report data to download.
        :return: Dict. Results of report request.
        """

        request = self.service.reports().getFile(
            reportId=report_id_, reportFragment=report_fragment_)
        data = request.execute()
        return data

    def __build_data_frame(self, csv_data_):
        """
        Generates an SDCDataframe with schema and loads report data in.
        :param csv_data_: String. Report data as comma-separated values.
        :return: SDCDataframe object.
        """

        pandas_df = pd.read_csv(StringIO(csv_data_), skipinitialspace=True)
        flat_data = pandas_df.to_dict('records')
        
        schema_file_name = SDCFileHelpers.get_file_path(
            "schema", f"Google/SearchAds360/{self.schemaName}.json")
        schema = json.loads(open(schema_file_name).read())
        df = Dataframe(SDCDFTypes.PANDAS, schema)
        df.load_data(flat_data)

        return df
    
    def get_report(self, schema_name_, params_=None, body_=None, num_of_attempts_=100, time_between_attempts_=10):
        """
        Retrieves Search Ads 360 report data.
        For more information on reports, see: "https://developers.google.com/search-ads/v2/reference/reports"
        :param schema_name_: String. Name of schema file to use for SDCDataframe. Do not include file extension.
        :param num_of_attempts_: Int. After a successful report request is made, calls are made to the API to determine
            if it is ready for downloading. This is the total number of times we want to attempt to check if ready before
            terminating. Default = 100.
        :param time_between_attempts_: Int. Number of seconds to wait in-between report request check calls. Default = 10.
        :param body_: Dict. Complete body message to make report request with. If None, body will be constructed from
            params provided from Class attributes.
        :param params_: Dict. Parameters to construct report request body from. Fields to include:
            Mandatory:
                reportType: String. Google report type. Possible values: "https://developers.google.com/search-ads/v2/report-types"
                columns: List. Columns for report. Possible values under each report type: "https://developers.google.com/search-ads/v2/report-types"
            Optional:
                advertiserId: String. Advertiser ID to filter results by. Default: None.
                filters: List. Filters to apply in report. Default: None.
                startDate: String. Start date for data, in a format of YYYY-MM-DD.
                endDate: String. End date for data, in a format of YYYY-MM-DD.
                advertiserId: String. Advertiser ID. Default: None.
                statisticsCurrency: String. Current output for report. Default: "usd".
                statisticsTimeZone: String. Timezone for report. Default: "UTC".
                changedMetricsSinceTimestamp: String of timestamp in RFC format (ex. '2013-07-16T10:16:23.555Z').
                    Requests metrics that have changed since the given timestamp. For more information:
                    "https://developers.google.com/search-ads/v2/how-tos/reporting/incremental-reports"
                changedAttributesSinceTimestamp: String of timestamp in RFC format (ex. '2013-07-16T10:16:23.555Z').
                    Requests attributes that have changed since the given timestamp (does not work for raw event reports
                    such as "conversion"). For more information:
                    "https://developers.google.com/search-ads/v2/how-tos/reporting/incremental-reports"
        :return: SDCDataframe object
        """

        self.schemaName = schema_name_

        if params_:
            self.reportType = params_.get("reportType")
            self.columns = params_.get("columns")
            self.filters = params_.get("filters")
            self.advertiserId = params_.get("advertiserId")
            self.startDate = params_.get("startDate")
            self.endDate = params_.get("endDate")
            self.changedAttributesSinceTimestamp = params_.get("changedAttributesSinceTimestamp")
            self.changedMetricsSinceTimestamp = params_.get("changedMetricsSinceTimestamp")

            if not self.reportType:
                logging.error(f'"params_" must include "reportType" attribute')
                raise ValueError(f'"params_" must include "reportType" attribute')
            if not self.columns:
                logging.error(f'"params_" must include "columns" attribute')
                raise ValueError(f'"params_" must include "columns" attribute')

        try:
            report_id_ = self.__request_report(body_=body_)
        except Exception as e:
            logging.error(f'Something went wrong. All we know is this: "{e}"')
            raise

        is_report_ready = False
        logging.info(f"Attempting to check on report. Max number of attempts: {num_of_attempts_}. "
                     f"Time between attempts: {time_between_attempts_} secs")
        check_start = time.time()

        for _ in range(num_of_attempts_):
            time.sleep(time_between_attempts_)
            ping = self.__get_report_status(report_id_)
            is_report_ready = ping["isReportReady"]
            files = ping["files"]
            if is_report_ready is True:
                break

        if not is_report_ready:
            check_end = time.time()
            raise Exception(f"Report was not ready after {num_of_attempts_:,} attempts and "
                            f"{round((check_end - check_start)/60, 2):,} minutes.")

        out = []
        f_cnt = len(files)
        for f in range(f_cnt):
            content = self.__fetch_report(report_id_, f)
            content = content.decode('utf-8')
            out.append(content)
        out = "\n".join(out)
        df = self.__build_data_frame(out)

        return df
    
    def __request_conversions(self):
        """
        Makes a request to Google for conversion data.
        :return: Dict. Result of request.
        """

        request = self.service.conversion().get(
            agencyId=self.agencyId,
            advertiserId=self.advertiserId,
            engineAccountId=self.engineAccountId,
            startDate=self.startDate,
            endDate=self.endDate,
            startRow=self.startRow,
            rowCount=self.rowCount
        )

        data = request.execute()
        return data

    def get_conversion(self, schema_name_, params_):
        """
        Retrieves Search Ads 360 conversion data.
        :param schema_name_: String. Name of schema file to use for SDCDataframe. Do not include file extension.
        :param params_: A dict object of:
            Mandatory:
                schemaName: String. Name of the file containing the schema that will be used to generate the SDCDataframe.
                advertiserId:
                engineAccountId:
                startDate: Value must be between 20091101 and 99991231.
                endDate: Value must be between 20091101 and 99991231.
                startRow: Zero indexed starting row.
                rowCount: Value must be between 0 and 1000.
            Optional:

        """
        self.schemaName = schema_name_
        self.advertiserId = params_.get("advertiserId")
        self.engineAccountId = params_.get("engineAccountId")
        self.startDate = params_.get("startDate")
        self.endDate = params_.get("endDate")
        self.startRow = params_.get("startRow")
        self.rowCount = params_.get("rowCount")

        if not self.schemaName:
            logging.error(f'"params_" must include "schemaName" attribute')
            raise ValueError(f'"params_" must include "schemaName" attribute')
        if not self.advertiserId:
            logging.error(f'"params_" must include "advertiserId" attribute')
            raise ValueError(f'"params_" must include "advertiserId" attribute')
        if not self.engineAccountId:
            logging.error(f'"params_" must include "engineAccountId" attribute')
            raise ValueError(f'"params_" must include "engineAccountId" attribute')
        if not self.startDate:
            logging.error(f'"params_" must include "startDate" attribute')
            raise ValueError(f'"params_" must include "startDate" attribute')
        if not self.endDate:
            logging.error(f'"params_" must include "endDate" attribute')
            raise ValueError(f'"params_" must include "endDate" attribute')
        if not self.startRow:
            logging.error(f'"params_" must include "startRow" attribute')
            raise ValueError(f'"params_" must include "startRow" attribute')
        if not self.rowCount:
            logging.error(f'"params_" must include "rowCount" attribute')
            raise ValueError(f'"params_" must include "rowCount" attribute')
        if not 0 <= int(self.rowCount) <= 1000:
            logging.error(f'"rowCount" value must be between 0 and 1000')
            raise ValueError(f'"rowCount" value must be between 0 and 1000')
        if not 20091101 <= int(self.startDate) <= 99991231:
            logging.error(f'"startDate" value must be between 20091101 and 99991231')
            raise ValueError(f'"startDate" value must be between 20091101 and 99991231')
        if not 20091101 <= int(self.endDate) <= 99991231:
            logging.error(f'"endDate" value must be between 20091101 and 99991231')
            raise ValueError(f'"endDate" value must be between 20091101 and 99991231')

        try:
            data = self.__request_conversions()
        except Exception as e:
            logging.error(f'It\'s dead, Jimmy. Its last words were: "{e}"')
            raise BaseException(f'It\'s dead, Jimmy. Its last words were: "{e}"')
        schema_file_name = SDCFileHelpers.get_file_path("schema", f"Google/SearchAds360/{self.schemaName}")
        schema = json.loads(open(schema_file_name).read())
        df = Dataframe(SDCDFTypes.PANDAS, schema)
        df.load_data(data["conversion"])

        return df
