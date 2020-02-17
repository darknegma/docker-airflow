
import collections
import json
import logging
import requests
import datetime
from dateutil import parser
from sdc_etl_libs.api_helpers.API import API
from sdc_etl_libs.sdc_dataframe.Dataframe import Dataframe, SDCDFTypes
from sdc_etl_libs.sdc_file_helpers.SDCFileHelpers import SDCFileHelpers

logging.basicConfig(level=logging.INFO)

class NewRelic(API):

    def __init__(self):

        self.credentials = self.get_credentials("aws_secrets", "new_relic/api")
        self.base_url = None
        self.headers = {'X-API-Key': self.credentials["api_key"]}

    def get_date_filter(self, datetime_, days_, periods_=3600):
        """
        Generates the dates portion of the New Relic API call URL.
        :param datetime_: Datetime object to serve as end date.
        :param days_: Number of days to go back from datetime_ to set as start
            date.
        :param periods_: Int. Method for breaking up metrics data into minutes,
        hours, etc. The ability to
            set this is governed by how many days are being pulled at one time.
            More info can be found here:
            https://docs.newrelic.com/docs/apis/rest-api-v2/requirements/extract-metric-timeslice-data#period
            Default is set to 3600, which returns 1-hour data slices when date
            range is less than 8 days.
        :return: URL date portion as string.
        """

        if type(datetime_) == str:
            datetime_ = parser.parse(datetime_)

        startdate = (datetime_ - datetime.timedelta(days_)).strftime('%Y-%m-%dT01:00:00+00:00')
        enddate = datetime_.strftime('%Y-%m-%dT01:00:00+00:00')
        url_date_string = f"from={startdate}&to={enddate}&period={periods_}"

        return url_date_string

    def get_fields_filter(self, names_, values_):
        """
        Generates the names and values portion of the New Relic API call URL.
        :param names_: List of metric names requested.
        :param values_: List of values requested.
        :return: URL string with names and values properly formatted,
        """

        if not isinstance(names_, list) and not isinstance(values_, list):
            raise Exception("Args names_ and values_ must be a Lists")

        names = ["names[]={}".format(i) for i in names_]
        values = ["values[]={}".format(i) for i in values_]
        names.extend(values)
        url_fields_string = f"&".join(names)

        return url_fields_string

    def flatten_metrics_data(self, json_data_):
        """
        Flattens the New Relic metrics timeslice data so it can be loaded into
        the SDCDataframe object.

        New Relics metric data is returned in the below format. The "metrics"
        portion of the data is passed to this function for flattening, which
        will convert:

          {
            "metrics": [
              {
                "name": "EndUser/UserAgent/Mobile/Browser",
                "timeslices": [
                  {
                    "from": "2015-10-01T01:00:00+00:00",
                    "to": "2015-10-04T01:00:00+00:00",
                    "values": {
                      "average_fe_response_time": 0,
                      "average_be_response_time": 0
                    }
                  }
                ]
              }
            ]
          }


        to:

        [
          {
            "name": "EndUser/UserAgent/Mobile/Browser",
            "from": "2015-10-01T01:00:00+00:00",
            "to": "2015-10-04T01:00:00+00:00",
            "average_fe_response_time": 0,
            "average_be_response_time": 0
          }
        ]

        :param json_data_: List of JSON records to be flattened.
        :return: List of flattened dictionary records.
        """

        def flatten_dictionary(json_data_):
            """
            Flattens JSON data into list of tuples.
            :param json_data_: JSON data to be flattened.
            :return: List of tuples.
            """
            items = []
            for key, value in json_data_.items():
                if isinstance(value, collections.MutableMapping):
                    items.extend(flatten_dictionary(value).items())
                else:
                    items.append((key, value))
            return dict(items)

        results = []
        for metric in json_data_:
            for item in metric["timeslices"]:
                temp = {}
                temp["name"] = metric["name"]
                temp.update(flatten_dictionary(item))
                results.append(temp)

        return results

    def process_metrics_endpoint(self, base_endpoint_url_, date_filter_=None,
                                 fields_filter_=None):
        """
        Processes New Relic metric data request. A New Relic API metrics response
        looks like the below. The data from "metric_data"."metrics" is returned by
        this function in JSON format.

        {
          "metric_data": {
            "from": "2015-10-01T01:00:00+00:00",
            "to": "2019-10-31T01:00:00+00:00",
            "metrics_not_found": [],
            "metrics_found": [
              "EndUser/UserAgent/Mobile/Browser",
              "EndUser/UserAgent/Tablet/Browser",
              "EndUser/UserAgent/Desktop/Browser"
            ],
            "metrics": [
              {
                "name": "EndUser/UserAgent/Mobile/Browser",
                "timeslices": [
                  {
                    "from": "2015-10-01T01:00:00+00:00",
                    "to": "2015-10-04T01:00:00+00:00",
                    "values": {
                      "average_fe_response_time": 0,
                      "average_be_response_time": 0
                    }
                  }
                ]
              }
            ]
          }
        }

        :param base_endpoint_url_: New Relic API base URL.
        :param date_filter_: Date filter as string.
        :param fields_filter_: Fields (names and values requested) as string.
        :return: List of JSON records.
        """

        data = []
        requests_url = f"{base_endpoint_url_}" \
            f"{fields_filter_ if fields_filter_ else ''}" \
            f"{'&'+date_filter_ if date_filter_ else ''}"

        logging.info(requests_url)

        r = requests.get(requests_url, headers=self.headers)

        if r.status_code == 200:
            try:
                data_json = json.loads(r.content)
                if not data_json:
                    logging.info(f"No results.")
            except Exception as e:
                logging.error(e)
                raise Exception(f"Unable to process data: {r.content}")

            for item in data_json["metric_data"]["metrics"]:
                data.append(item)

            logging.info(f"Grabbed {len(data_json):,} record(s).")
        else:
           raise Exception(
               f"Failed to get access group data from api. "
               f"Status: {r.status_code}")

        return data

    def get_metrics_data(self, data_schema_name_, date_filter_=None,
                         fields_filter_=None):
        """
        Initiatives a New Relic API request - processing the data, flattening and
        loading into an SDCDataframe object.
        :param data_schema_name_: JSON schema file name for data.
        :param date_filter_: Date filter as string.
        :param fields_filter_: Fields (names and values requested) as string.
        :return: SDCDataframe object.
        """

        file_name = SDCFileHelpers.get_file_path(
            "schema", f"NewRelic/{data_schema_name_}.json")

        json_data = json.loads(open(file_name).read())
        if "data_source" in json_data and json_data["data_source"]["type"] == "api":
            self.base_url = json_data["data_source"]["base_url"]
        else:
            raise Exception(
                "Missing data_source metadata in schema definition.")

        df = Dataframe(SDCDFTypes.PANDAS, json_data)

        requests_url = self.base_url + f'/metrics/data.json?'

        data = self.process_metrics_endpoint(requests_url, date_filter_,
                                             fields_filter_)

        flattened_data = self.flatten_metrics_data(data)

        if len(data) >= 1:
            df.load_data(flattened_data)
            return df

        else:
            logging.warning("Received no data.")
            return None
