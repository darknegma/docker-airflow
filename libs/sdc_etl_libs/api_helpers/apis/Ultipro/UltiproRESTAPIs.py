
import datetime
from dateutil import parser
import json
import logging
import requests
from sdc_etl_libs.api_helpers.apis.Ultipro.Ultipro import Ultipro
from sdc_etl_libs.sdc_dataframe.Dataframe import Dataframe, SDCDFTypes
from sdc_etl_libs.sdc_file_helpers.SDCFileHelpers import SDCFileHelpers

logging.basicConfig(level=logging.INFO)

class UltiproRESTAPIs(Ultipro):

    def __init__(self):
        super().__init__()
        self.rest_authenticate("username", "password")

    def get_daily_filter(self, datetime_, days_=1, type_=1, field_=None):
        """
        This function creates a date filter of the previous day.
        :param datetime_: Datetime object tp serve as end date.
        :param days_: Number of days to go back from datetime_ to set as start date.
        :param type_: Type of filter to generate. Date range is between the
                start date (date_time less the days_) to the end date (datetime_)

                Options include:
                1 = Generates URL filter string with the field_ name provided.
                2 = Generates URL filter string with literal "startDate"
                    and "endDate" fields used.

        :param field_: List of fields to create complex filter with (with type 1 filter only)
        :return: Filter string of a date range to use in request URL.
        """

        if type(datetime_) == str:
            datetime_ = parser.parse(datetime_)

        if type_ == 1:

            if not field_:
                raise Exception("Must provide field for Type 1 filtering on REST API")

            startdate = (datetime_ - datetime.timedelta(days_)).strftime("%Y-%m-%d")
            enddate = datetime_.strftime("%Y-%m-%d")
            url_filter = f'{field_}={{{startdate}T00:00:00,{enddate}T23:59:59}}'
            return url_filter

        elif type_ == 2:

            startdate = (datetime_ - datetime.timedelta(days_)).strftime("%Y/%m/%d")
            enddate = datetime_.strftime("%Y/%m/%d")
            url_filter = f'startDate={startdate}T00:00:00&endDate={enddate}T23:59:59'
            return url_filter

        else:
            raise Exception(f"Filter type {type_} not supported.")

    def process_endpoint(self, base_endpoint_url_, filter_, limit_=None):
        """
        This function handles the pagination of the ultipro api calls.
        :param base_endpoint_url_: base url for the api : string
        :param filter_: filter for api endpoint: string
        :param limit_: How many records to return per page : int.
            NOTE: Different endpoints have different limits. If set too high,
            the API will either automatically set it to the max, or, error out.
        :return: List of dictionary results.
        """

        data = []
        page = 1
        while True:
            if page > 1000:
                break
            if filter_ is not None:
                requests_url = f"{base_endpoint_url_}?{filter_}&page={page}"
            else:
                requests_url = f"{base_endpoint_url_}?page={page}"

            if limit_ is not None:
                if type(limit_) == int:
                    requests_url = f"{requests_url}&per_page={limit_}"
                else:
                    raise Exception("limit_ must be of type integer")

            r = requests.get(requests_url, auth=self.auth,
                             headers={"US-Customer-Api-Key":self.credentials["api_key"]})

            if r.status_code == 200:
                try:
                    data_json = json.loads(r.content)
                except Exception as e:
                    logging.error(e)
                    raise Exception(f"Unable to process data: {r.content}")

                if len(data_json) < 1:
                    break

                for item in data_json:
                    data.append(item)

                logging.info(f"Grabbed {len(data_json):,} record(s) from page "
                             f"{page}. {len(data):,} total records so far.")

            else:
                raise Exception(
                    f"Failed to get data from api. status: {r.status_code}")

            page += 1

        return data

    def get_employment_details(self, filter_=None, limit_=100):
        """
        This function grabs data from the employment-details api and returns a dataframe.
        :param filter_: Specific filters to narrow down queried data.
        :param limit_: How many records to return per page. Default (and endpoint max) is 100.
        https://connect.ultipro.com/documentation#/api/817/EmploymentDetails/get__personnel_v1_employment-details
        :return: Dataframe
        """
        schema_name = "Ultipro.employment-details"
        file_name = SDCFileHelpers.get_file_path(
            "schema", "Ultipro/rest_apis/employment-details.json")
        json_data = json.loads(open(file_name).read())
        if "data_source" in json_data and json_data["data_source"]["type"] == "api":
            self.base_url = json_data["data_source"]["base_url"]
        else:
            raise Exception("Missing data_source metadata in schema definition.")

        df = Dataframe(SDCDFTypes.PANDAS,json_data)

        base_endpoint_url = self.base_url + '/employment-details'

        data = self.process_endpoint(base_endpoint_url, filter_, limit_)

        if len(data) >= 1:
            df.load_data(data)
            return df
        else:
            logging.warning("Received no data")
            return None

    def get_employee_changes(self, filter_=None, limit_=200):
        """
        This function will get data from the employment changes api and return a dataframe
        :param filter_: Specific filters to narrow down queried data.
        :param limit_: How many records to return per page. Default (and endpoint max) is 200.
        https://connect.ultipro.com/documentation#/api/199/Changes%20By%20Date/get__personnel_v1_employee-changes
        :return: Dataframe
        """

        schema_name = "Ultipro.employee-changes"
        file_name = SDCFileHelpers.get_file_path(
            "schema", "Ultipro/rest_apis/employee-changes.json")
        json_data = json.loads(open(file_name).read())
        if "data_source" in json_data and json_data["data_source"]["type"] == "api":
            self.base_url = json_data["data_source"]["base_url"]
        else:
            raise Exception("Missing data_source metadata in schema definition.")
        df = Dataframe(SDCDFTypes.PANDAS, json_data)

        base_endpoint_url = self.base_url + '/employee-changes'

        data = self.process_endpoint(base_endpoint_url, filter_, limit_)

        if len(data) >= 1:
            df.load_data(data)
            return df
        else:
            logging.warning("Received no data")
            return None

    def get_compensation_details(self, filter_=None, limit_=100):
        """
        This functions grabs the compensation-details api and returns a dataframe.
        :param filter_: Specific filters to narrow down queried data.
        :param limit_: How many records to return per page. Default (and endpoint max) is 200.
        https://connect.ultipro.com/documentation#/api/823/CompensationDetails/get__personnel_v1_compensation-details
        :return: Dataframe
        """

        schema_name = "Ultipro.compensation-details"
        file_name = SDCFileHelpers.get_file_path(
            "schema", "Ultipro/rest_apis/compensation-details.json")
        json_data = json.loads(open(file_name).read())
        if "data_source" in json_data and json_data["data_source"]["type"] == "api":
            self.base_url = json_data["data_source"]["base_url"]
        else:
            raise Exception("Missing data_source metadata in schema definition.")
        df = Dataframe(SDCDFTypes.PANDAS,json_data)

        base_endpoint_url = self.base_url + '/compensation-details'

        data = self.process_endpoint(base_endpoint_url, filter_, limit_)

        if len(data) >= 1:
            df.load_data(data)
            return df
        else:
            logging.warning("Received no data")
            return None

    def get_pto_plans(self, filter_=None, limit_=1000):
        """
        This functions grabs the pto-plans api and returns a SDCDataframe object
        with data loaded into the dataframe if data is available.
        :param filter_: Specific filters to narrow down queried data.
        :param limit_: How many records to return per page. Default (and endpoint max) is 1,000.

        https://connect.ultipro.com/documentation#/api/721/
        :return: Dataframe
        """

        schema_name = "Ultipro.pto-plans"
        file_name = SDCFileHelpers.get_file_path(
            "schema", "Ultipro/rest_apis/pto-plans.json")
        json_data = json.loads(open(file_name).read())
        if "data_source" in json_data and json_data["data_source"]["type"] == "api":
            self.base_url = json_data["data_source"]["base_url"]
        else:
            raise Exception("Missing data_source metadata in schema definition.")
        df = Dataframe(SDCDFTypes.PANDAS, json_data)

        base_endpoint_url = self.base_url + '/pto-plans'

        data = self.process_endpoint(base_endpoint_url, filter_, limit_)

        if len(data) >= 1:
            df.load_data(data)
            return df
        else:
            logging.warning("Received no data")
            return None

    def get_person_details(self, filter_=None, limit_=100):
        """
        This functions grabs the person-details api and returns a
        SDCDataframe object with data loaded into the dataframe if data is available.
        :param filter_: Specific filters to narrow down queried data.
        :param limit_: How many records to return per page. Default (and endpoint max) is 100.

        https://connect.ultipro.com/documentation#/api/811/PersonDetails/get__personnel_v1_person-details
        :return: Dataframe
        """

        schema_name = "Ultipro.person-details"
        file_name = SDCFileHelpers.get_file_path(
            "schema", "Ultipro/rest_apis/person-details.json")
        json_data = json.loads(open(file_name).read())
        if "data_source" in json_data and json_data["data_source"]["type"] == "api":
            self.base_url = json_data["data_source"]["base_url"]
        else:
            raise Exception("Missing data_source metadata in schema definition.")
        df = Dataframe(SDCDFTypes.PANDAS, json_data)

        base_endpoint_url = self.base_url + '/person-details'

        data = self.process_endpoint(base_endpoint_url, filter_, limit_)

        if len(data) >= 1:
            df.load_data(data)
            # Drop SSN related columns so they do not get loaded into database.
            df.drop_columns(["SSN", "PREVIOUSSSN"])
            return df
        else:
            logging.warning("Received no data")
            return None

    def get_employee_job_history_details(self, filter_=None, limit_=200):
        """
        This functions grabs the employee-job-history-details api and returns a
        SDCDataframe object with data loaded into the dataframe if data is available.
        :param filter_: Specific filters to narrow down queried data.
        :param limit_: How many records to return per page. Default (and endpoint max) is 200.

        https://connect.ultipro.com/documentation#/api/1468
        :return: Dataframe
        """

        schema_name = "Ultipro.employee-job-history-details"
        file_name = SDCFileHelpers.get_file_path(
            "schema", "Ultipro/rest_apis/employee-job-history-details.json")
        json_data = json.loads(open(file_name).read())
        if "data_source" in json_data and json_data["data_source"]["type"] == "api":
            self.base_url = json_data["data_source"]["base_url"]
        else:
            raise Exception("Missing data_source metadata in schema definition.")
        df = Dataframe(SDCDFTypes.PANDAS, json_data)

        base_endpoint_url = self.base_url + '/employee-job-history-details'

        data = self.process_endpoint(base_endpoint_url, filter_, limit_)

        if len(data) >= 1:
            df.load_data(data)
            return df
        else:
            logging.warning("Received no data")
            return None
