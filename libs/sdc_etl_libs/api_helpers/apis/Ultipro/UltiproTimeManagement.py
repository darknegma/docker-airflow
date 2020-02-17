
import datetime
from dateutil import parser
import json
import logging
import requests
from sdc_etl_libs.api_helpers.apis.Ultipro.Ultipro import Ultipro
from sdc_etl_libs.sdc_dataframe.Dataframe import Dataframe, SDCDFTypes
from sdc_etl_libs.sdc_file_helpers.SDCFileHelpers import SDCFileHelpers

logging.basicConfig(level=logging.INFO)

class UltiproTimeManagement(Ultipro):

    def __init__(self):
        super().__init__()
        self.rest_authenticate("tm_username", "tm_password")

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

        startdate = (datetime_ - datetime.timedelta(days_)).strftime("%Y-%m-%d")
        enddate = datetime_.strftime("%Y-%m-%d")
        url_filter = \
            " or ".join(f'({field} ge {startdate} '
                        f'and {field} le {enddate})' for field in filter_field_list_)
        return url_filter

    def process_endpoint(self, base_endpoint_url_, filter_, limit_=1000):
        """
        This function handles the pagination of the Ultipro API calls for
        Time Management UTMOData Service.

        :param base_endpoint_url_: base url for the api : string
        :param filter_: filter for api endpoint: string
        :param limit_: Number of records to return with each call. Default is 1,000.
        :return: List of JSON values
        """

        data = []

        if filter_ is not None:
            requests_url = f"{base_endpoint_url_}?$filter={filter_}&$count=true"
        else:
            requests_url = f"{base_endpoint_url_}?&$count=true"

        total_records = 1
        skip = 0

        while skip < total_records:

            requests_url_with_pagination = requests_url + \
                                           f"&$skip={skip}&$top={limit_}"

            logging.info(requests_url_with_pagination)

            r = requests.get(requests_url_with_pagination, auth=self.auth)

            if r.status_code == 200:
                try:
                    data_json = json.loads(r.content)
                    total_records = data_json["@odata.count"]
                    records = data_json["value"]
                    if skip == 0:
                        logging.info(f"Total records to grab: {total_records:,}")
                    skip += limit_
                except Exception as e:
                    logging.error(e)
                    raise Exception(f"Unable to process data: {r.content}")

                for item in records:
                    data.append(item)

                logging.info(f"Grabbed {len(records):,} record(s) from page "
                             f"{int(skip/limit_)}. Progess: "
                             f"{len(data):,}/{total_records:,}")
            else:
                raise Exception(
                    f"Failed to get access group data from api. "
                    f"Status: {r.status_code}")

        return data

    def get_data_from_endpoint(self, schema_name_, endpoint_name_,
                               filter_=None, limit_=1000):
        """
        Grabs data from API endpoint and returns a SDCDataframe object with
        data loaded into the dataframe if data is avaliable.

        :param schema_name_: Schema file name for SDCDataframe instance..
        :param endpoint_name_: Name of API URL endpoint.
        :param filter_: Filter to apply to API call.
        :return: SDCDataframe object with data in dataframe.
        """

        file_name = SDCFileHelpers.get_file_path(
            "schema", f"Ultipro/time_management/{schema_name_}.json")
        json_data = json.loads(open(file_name).read())
        if "data_source" in json_data and json_data["data_source"][
            "type"] == "api":
            self.base_url = json_data["data_source"]["base_url"]
        else:
            raise Exception(
                "Missing data_source metadata in schema definition.")
        df = Dataframe(SDCDFTypes.PANDAS, json_data)

        base_endpoint_url = self.base_url + f'/{endpoint_name_}'

        data = self.process_endpoint(base_endpoint_url, filter_, limit_)

        if len(data) >= 1:
            df.load_data(data)
            return df
        else:
            logging.warning("Received no data")
            return None
