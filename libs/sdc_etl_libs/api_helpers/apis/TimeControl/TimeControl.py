
import json
import logging
import requests
import datetime
from dateutil import parser
from sdc_etl_libs.api_helpers.API import API
from sdc_etl_libs.sdc_dataframe.Dataframe import Dataframe, SDCDFTypes
from sdc_etl_libs.sdc_file_helpers.SDCFileHelpers import SDCFileHelpers


logging.basicConfig(level=logging.INFO)


class TimeControl(API):

    def __init__(self):
        self.credentials = self.get_credentials("aws_secrets", "timecontrol/api")
        self.headers = {'X-API-Key': self.credentials["apikey"]}

    def get_daily_filter(self, datetime_, days_, filter_field_list_):
        """
        Constructs a filter for Time Management calls using the list of supplied
        fields. A start date and end date is set with the fields used with
        "greater than or equal to" start date and "less than" end date. When
        there is more than one field they are combined with 'or'.

        :param datetime_: Datetime object tp serve as end date.
        :param days_: Number of days to go back from datetime_ to set as start date.
        :param filter_field_list_: List of fields to create complex filter with.
        :return: URL filter as string.
        """

        if not isinstance(filter_field_list_, list):
            raise Exception("fields_ must be a list for Time Management "
                            "get_daily_filter()")

        if type(datetime_) == str:
            datetime_ = parser.parse(datetime_)

        startdate = (datetime_ - datetime.timedelta(days_)).strftime("%Y-%m-%dT00:00:00")
        enddate = datetime_.strftime("%Y-%m-%dT00:00:00")
        url_filter = \
            " or ".join(f"({field} ge DateTime'{startdate}' "
                        f"and {field} lt DateTime'{enddate}')" for field in filter_field_list_)

        return url_filter

    def get_key_filter(self, start_key_, json_key_path_="Key"):
        """
        Creates a filter based on the primary Key identifier from the endpoint.
        Filter will be greater than or equal to the provided start_key_.

        :param start_key_: Int. Key number to start from.
        :param json_key_path_: The Key path in the JSON returned from the endpoint.
        Default is "Key". Keys can be selected from within the nested JSON by
        using parentheses to traverse the tree. Example:
            Employee/Timesheets/Key
        :return: URL filter as a string.
        """

        if not isinstance(start_key_, int):
            raise Exception("start_key_ must be an int for get_key_filter()")

        url_filter = f"{json_key_path_} ge {start_key_}"

        return url_filter

    def process_endpoint(self, base_endpoint_url_, filter_=None, limit_=1000):
        """
        Processes a TimeControl API endpoint.
        :param base_endpoint_url_: API base endpoint URL.
        :param filter_: Filter as URL string (OData specfication). Default = None.
            https://www.odata.org/documentation/odata-version-3-0/url-conventions/
        :param limit_: Int. Number of records to return each page. Deafult = 1000.
        :return: Data as a list of flattened dictionaries.
        """

        data = []
        skip = 0
        page = 1
        data_json = ['start']

        while data_json and page < 1000:

            requests_url_with_pagination = f"{base_endpoint_url_}?$skip={skip}&$top={limit_}" \
                f"{'&$filter='+filter_ if filter_ else ''}"

            logging.info(requests_url_with_pagination)

            r = requests.get(requests_url_with_pagination, headers=self.headers)

            if r.status_code == 200:
                try:
                    data_json = json.loads(r.content)
                    skip += limit_
                    page = int(skip / limit_)
                    if not data_json:
                        logging.info(f"No results from page {page}.")
                        break
                except Exception as e:
                    logging.error(e)
                    raise Exception(f"Unable to load data into JSON format.")

                for item in data_json:
                    data.append(item)

                logging.info(f"Grabbed {len(data_json):,} record(s) from page "
                             f"{page}.")
            else:
                raise Exception(
                    f"Failed to get access group data from api. "
                    f"Status: {r.status_code}")

        return data

    def get_data(self, data_schema_name_, endpoint_, filter_=None, limit_=1000):
        """
        Grabs data from a TimeControl endpoint and returns as an
        SDCDataframe object if data is avaiable.
        :param data_schema_name_:
        :param endpoint_: TimeControl API endpoint, which will be appended to
            base URL.
        :param filter_: Filter as URL string. Deafult = None.
        :param limit_: Int. Number of records to return per page. Default = 1000.
        :return: SDCDataframe object if data, else, None.
        """

        file_name = SDCFileHelpers.get_file_path(
            "schema", f"TimeControl/{data_schema_name_}.json")

        json_data = json.loads(open(file_name).read())
        if "data_source" in json_data and json_data["data_source"]["type"] == "api":
            self.base_url = json_data["data_source"]["base_url"]
        else:
            raise Exception("Missing data_source metadata in schema definition.")

        df = Dataframe(SDCDFTypes.PANDAS, json_data)

        requests_url = self.base_url + f'/{endpoint_}'

        data = self.process_endpoint(requests_url, filter_, limit_)

        if len(data) >= 1:
            df.load_data(data)
            return df

        else:
            logging.warning("Received no data.")
            return None
