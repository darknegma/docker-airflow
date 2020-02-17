
import json
import logging
import requests
import datetime
from dateutil import parser
from ast import literal_eval
from sdc_etl_libs.api_helpers.API import API
from sdc_etl_libs.sdc_dataframe.Dataframe import Dataframe, SDCDFTypes
from sdc_etl_libs.sdc_file_helpers.SDCFileHelpers import SDCFileHelpers

logging.basicConfig(level=logging.INFO)


class Podium(API):

    def __init__(self):
        self.credentials = self.get_credentials("aws_secrets", "podium/api")
        self.auth = literal_eval(
            "{'api_token':'" + self.credentials['key'] + "'}")

    def get_date_range_filter(self, datetime_, num_of_days_):
        """
        This function creates a date filter of the previous day.

        :param datetime_: Dateime object or a string that represents a datetime.
            Used as the end date of the date range + one day.
        :param num_of_days_: Integer. Number of days to go back from datetime_
            to set as starting date for range.
        :return: A filter string that is the data from the previous day.
        """
        if type(datetime_) == str:
            datetime_ = parser.parse(datetime_)
        enddate = (datetime_ + datetime.timedelta(1)).strftime("%Y-%m-%d")
        startdate = (datetime_ - datetime.timedelta(num_of_days_))\
            .strftime("%Y-%m-%d")
        url_filter = f'fromDate={startdate}&toDate={enddate}'
        return url_filter

    def process_endpoint(self, endpoint_, data_name_, filter_):
        """
        This function handles the processing of Podium endpoints including
        pagination.

        :param endpoint_: A URL string for the desired endpoint.
        :param data_name_: Name of the data in the JSON result. This is used
            to grab the data under the first level of the results. (i.e., the
            parent node of the reviews endpoint will be "review")
        :param filter_: A url filter used to filter data.
        :param limit_: The number of results returned by the API
        :return: list of dictionary objects.
        """

        page = 1
        data = []

        while True:

            if filter_:
                requests_url = \
                    f"{endpoint_}?page[size]=200&{filter_}&page[number]={str(page)}"
            else:
                requests_url = \
                    f"{endpoint_}?page[size]=200&page[number]={str(page)}"

            r = requests.get(requests_url, data=self.auth)

            if r.status_code == 200:
                data_json = json.loads(r.content)

                data += data_json[data_name_]

                if int(data_json["meta"]["page"]["number"]) < \
                        int(data_json["meta"]["page"]["total"]):
                    page += 1
                else:
                    break

            else:
                raise Exception(f"Call failed due to API Error {r.status_code}")

        return data

    def get_locations(self, filter_=None):
        """
        Grabs Podium Locations data and puts into a dataframe. Also, updates
        an Airflow variable so other API calls can ue the list of locations
        for their endpoint urls.

        :param filter_: String to append to endpoint url that represents filter.
        :return: Dataframe
        """

        file_name = SDCFileHelpers.get_file_path("schema",
                                                  "Podium/locations.json")

        json_data = json.loads(open(file_name).read())
        if "data_source" in json_data \
                and json_data["data_source"]["type"] == "api":
            self.base_url = json_data["data_source"]["base_url"]
        else:
            raise Exception(
                "Missing data_source metadata in schema definition.")

        df = Dataframe(SDCDFTypes.PANDAS, json_data)

        base_endpoint_url = self.base_url + '/organizations/9119/locations'

        data = self.process_endpoint(base_endpoint_url, "locations", filter_)

        if len(data) >= 1:
            df.load_data(data)
            return df

        else:
            logging.warning("Received no data.")
            return None

    def get_reviews(self, locations_, filter_=None):
        """
        Grabs Podium Reviews data and puts into dataframe.

        :param locations_: List of locations to iterate over API call.
        :param filter_: String to append to endpoint url that represents filter.
        :return: Dataframe
        """

        file_name = SDCFileHelpers.get_file_path("schema",
                                                  "Podium/reviews.json")

        json_data = json.loads(open(file_name).read())
        if "data_source" in json_data \
                and json_data["data_source"]["type"] == "api":
            self.base_url = json_data["data_source"]["base_url"]
        else:
            raise Exception(
                "Missing data_source metadata in schema definition.")

        df = Dataframe(SDCDFTypes.PANDAS, json_data)
        data = []

        if locations_ and isinstance(locations_, list):

            for location in locations_:

                logging.info(f"Making API calls for location {location}...")

                base_endpoint_url = "{0}/locations/{1}/reviews".format(
                    self.base_url, str(location))

                data += self.process_endpoint(base_endpoint_url, "reviews",
                                              filter_)

            if len(data) >= 1:
                df.load_data(data)
                return df

            else:
                logging.warning("Received no data.")
                return None

        else:
            raise Exception("Must pass a list of location ids to make API call.")

    def get_invites(self, locations_, filter_=None):
        """
        Grabs Podium Invites data and puts into dataframe.

        :param locations_: List of locations to iterate over API call.
        :param filter_: String to append to endpoint url that represents filter.
        :return: Dataframe
        """

        file_name = SDCFileHelpers.get_file_path("schema",
                                                  "Podium/invites.json")

        json_data = json.loads(open(file_name).read())
        if "data_source" in json_data \
                and json_data["data_source"]["type"] == "api":
            self.base_url = json_data["data_source"]["base_url"]
        else:
            raise Exception(
                "Missing data_source metadata in schema definition.")

        df = Dataframe(SDCDFTypes.PANDAS, json_data)
        data = []

        if locations_ and isinstance(locations_, list):

            for location in locations_:

                logging.info(f"Making API calls for location {location}...")

                base_endpoint_url = f"{self.base_url}/locations/{str(location)}/invites"

                data += self.process_endpoint(base_endpoint_url, "invites", filter_)

            if len(data) >= 1:
                df.load_data(data)
                return df

            else:
                logging.warning("Received no data.")
                return None

        else:
            raise Exception("Must pass a list of location ids to make API call.")
