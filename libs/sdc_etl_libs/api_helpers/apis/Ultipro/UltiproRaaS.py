
import csv
import datetime
import logging
import json
import requests
import backoff
from zeep import Client as Zeep
from zeep.transports import Transport
from sdc_etl_libs.api_helpers.apis.Ultipro.Ultipro import Ultipro
from sdc_etl_libs.sdc_dataframe.Dataframe import Dataframe, SDCDFTypes
from sdc_etl_libs.sdc_file_helpers.SDCFileHelpers import SDCFileHelpers

logging.basicConfig(level=logging.INFO)

class UltiproRaaS(Ultipro):

    def __init__(self):
        super().__init__()
        self.report_run_date = None
        self.report_key = None
        self.base_url = None

    @backoff.on_exception(backoff.expo, requests.exceptions.HTTPError,
                          max_tries=8, on_backoff=Ultipro.soap_backoff_handler)
    def log_on_with_token(self):
        """
        Generates context for BIDataService and method for logging on with token.

        :return: Zeep service logon with token and context
        """

        logging.info(f"Logging on with token...")
        credentials = {
            'Token': self.token,
            'ClientAccessKey': self.credentials["api_key"]
        }

        zeep_client = Zeep(f"{self.base_url}BiDataService")
        element = zeep_client.get_element('ns5:LogOnWithTokenRequest')
        obj = element(**credentials)

        return zeep_client.service.LogOnWithToken(obj)

    @backoff.on_exception(backoff.expo, requests.exceptions.HTTPError,
                          max_tries=8, on_backoff=Ultipro.soap_backoff_handler)
    def get_report_list(self):
        """
        Grab list of available RaaS reports for this user and converts from
        Zeep object to standard list of dictionaries.

        :return: List of dictionary records.
        """

        report_details = []
        self.soap_authenticate()
        context = self.log_on_with_token()
        zeep_client = Zeep(f"{self.base_url}BiDataService")
        raw_results = zeep_client.service.GetReportList(context)

        for record in raw_results["Reports"]["Report"]:
            record_as_dict = {
                "ReportName": record["ReportName"],
                "ReportPath": record["ReportPath"]
            }

            report_details.append(record_as_dict)

        return report_details

    @backoff.on_exception(backoff.expo, requests.exceptions.HTTPError,
                          max_tries=8, on_backoff=Ultipro.soap_backoff_handler)
    def execute_report(self, context, report_path_, delimiter_=','):
        """
        Executes/runs a BIReport to refresh the data.

        :param context:
        :param report_path_: Path to BI Report in Ultipro. Example:
            "/content/folder[@name='zzzCompany Folders']/folder[@name='SmileDirectClub, LLC']/folder[@name='UltiPro']/folder[@name='Shared Folders']/folder[@name='IT']/report[@name='RaaS']"
        :param delimiter_: Delimiter character to use in report.
        :return: Report key as string.
        """
        session = requests.Session()
        session.headers.update({'US-DELIMITER': delimiter_})
        transport = Transport(session=session)
        payload = {'ReportPath': report_path_}
        zeep_client = Zeep(f"{self.base_url}BiDataService",transport=transport)
        element = zeep_client.get_element('ns5:ReportRequest')
        obj = element(**payload)
        r = zeep_client.service.ExecuteReport(request=obj, context=context)

        self.report_run_date = datetime.datetime.now()
        self.report_key = r['ReportKey']
        logging.info(f"Report ran at {self.report_run_date}. "
                     f"Report key: {self.report_key}")

    @backoff.on_exception(backoff.expo, requests.exceptions.HTTPError,
                          max_tries=8, on_backoff=Ultipro.soap_backoff_handler)
    def get_report_data(self, report_path_, delimiter_=','):
        """
        Triggers report execution for refersh then grabs report data via
        BIStreamingService.

        :param report_path_: Path to BI Report in Ultipro. Example:
            "/content/folder[@name='zzzCompany Folders']/folder[@name='SmileDirectClub, LLC']/folder[@name='UltiPro']/folder[@name='Shared Folders']/folder[@name='IT']/report[@name='RaaS']"
        :param delimiter_: Delimiter character to use in report.
        :return: Zeep array object.
        """

        self.soap_authenticate()
        context = self.log_on_with_token()
        logging.info("Executing report run...")
        self.execute_report(context, report_path_, delimiter_=delimiter_)
        zeep_client = Zeep(f"{self.base_url}BiStreamingService")
        logging.info("Retrieving report data...")
        result = zeep_client.service.RetrieveReport(
            _soapheaders={'ReportKey': self.report_key})

        data = result['body']['ReportStream'].decode('unicode-escape')

        return data

    def report_data_to_dict(self, raw_data_):
        """
        Converts Zeep array object of results to list of dictionary records.

        :param raw_data_: Raw data from Zeep call.
        :return: List of dictionary records.
        """

        logging.info("Transforming data into list of dictionaries...")
        data = []
        # Lines in report broken up with \r\n. Split up here.
        lines = raw_data_.splitlines()
        # Clean up header line to remove all spaces and make uppercase. Makes
        # easier for loading into dataframe
        lines[0] = lines[0].replace(' ', '').upper()

        reader = csv.DictReader(lines)
        for line in reader:
            data.append(dict(line))

        return data

    def process_report_data(self, report_path_, schema_file_name_):
        """
        Facilitates the complete RaaS report extraction process - refreshing,
        fetching, converting and loading into an SDCDataframe object.

        :param report_path_: Path to BI Report in Ultipro. Example:
            "/content/folder[@name='zzzCompany Folders']/folder[@name='SmileDirectClub, LLC']/folder[@name='UltiPro']/folder[@name='Shared Folders']/folder[@name='IT']/report[@name='RaaS']"
        :param schema_file_name_: SDCDataframe schema file name
        :return: Dataframe
        """

        file_name = SDCFileHelpers.get_file_path(
            "schema", f"Ultipro/raas/{schema_file_name_}.json")
        json_data = json.loads(open(file_name).read())
        if "data_source" in json_data and json_data["data_source"]["type"] == "api":
            self.base_url = json_data["data_source"]["base_url"]
        else:
            raise Exception(
                "Missing data_source metadata in schema definition.")

        self.soap_authenticate()

        df = Dataframe(SDCDFTypes.PANDAS, json_data)

        results = self.get_report_data(report_path_)
        data = self.report_data_to_dict(results)

        if len(data) >= 1:
            df.load_data(data)

            # Add in the ReportKey and RunDate so that the data can be queried
            # efficiently by report run and/or latest report
            df.df["REPORTDATE"] = self.report_run_date
            df.df["REPORTKEY"] = self.report_key
            return df
        else:
            logging.warning("Received no data")
            return None
