import os
import json
import logging
import datetime
from dateutil import parser
import pandas as pd
import suds

logging.basicConfig(level=logging.INFO)

from sdc_etl_libs.api_helpers.API import API
from sdc_etl_libs.sdc_dataframe.Dataframe import Dataframe, SDCDFTypes
from sdc_etl_libs.sdc_file_helpers.SDCFileHelpers import SDCFileHelpers
import FuelSDK


class ExactTarget(API):
    def __init__(self, credential_type_="aws_secrets"):
        # secrets in the from of {"username":,"password":, "api_key": }
        # self.base_url = base_url_

        self.credentials = self.get_credentials(credential_type_,
                                                "exact-target/api")

        self.client = FuelSDK.ET_Client(params=self.credentials)

    def __suds_to_dict(self, suds_obj_):
        """
        Converts suds object to dict.
        Note: this function uses recursion
        :param suds_obj: suds object
        :return: dict object
        """
        if not hasattr(suds_obj_, '__keylist__'):
            if not isinstance(suds_obj_, type(None)):
                if isinstance(suds_obj_, suds.sax.text.Text):
                    return str(suds_obj_)
                elif isinstance(suds_obj_, datetime.time) or isinstance(suds_obj_, datetime.datetime):
                    return str(suds_obj_)
                else:
                    return suds_obj_
            else:
                return None
        data = {}
        fields = suds_obj_.__keylist__
        for field in fields:
            value = getattr(suds_obj_, field)
            if isinstance(value, list):
                data[field] = []
                #recreate the list, converting objects within recursively.
                for item in value:
                    data[field].append(self.__suds_to_dict(item))
            else:
                data[field] = self.__suds_to_dict(value)

        return data

    def normalize_data(self, data_):
        for index in range(len(data_)):
            data_[index] = pd.io.json.json_normalize(self.__suds_to_dict(data_[index]), sep='_').to_dict(orient='records')[0]
        return data_

    def or_two_filters(self, filter1_, filter2_):
        """
        :param filter1_: A filter returned by the
        get_filter_for_last_n_minutes function
        :param filter2_: A filter returned by the
        get_filter_for_last_n_minutes function
        :return: a complex filter dict that is logical "or" together..
        """
        out_filter = {'LeftOperand': filter1_, 'LogicalOperator': 'OR',
                      'RightOperand': filter2_}
        return out_filter


    def get_filter_for_last_n_minutes(self, date_property_, minutes_,
                                      datetime_):
        """
        This function creates a date filter of the previous n minutes.
        https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis
        .meta/mc-apis/creating_a_filterdefinition_object.htm
        :param date_propery_: property name that corresponds to date we want
        to filter by.
        :param minutes_: an integer that is the number of minutes in the past
        you want to cover.
        :param datetime_: dateime object or a string that represents a datetime.
        :return: a filter dict that is the data from the previous day.
        """
        if not isinstance(minutes_, int):
            raise Exception("Minutes must be an int for the filter.")
        if type(datetime_) == str:
            datetime_ = parser.parse(datetime_)

        from_dt = (datetime_ - datetime.timedelta(minutes=minutes_)).strftime(
            "%Y-%m-%dT%H:%M:%S")
        to_dt = datetime_.strftime("%Y-%m-%dT%H:%M:%S")

        logging.info(f"from {from_dt} to {to_dt}")

        from_filter = {'Property': date_property_,
                       'SimpleOperator': 'greaterThan',
                       'DateValue': from_dt}
        to_filter = {'Property': date_property_,
                     'SimpleOperator': 'lessThanOrEqual',
                     'DateValue': to_dt}
        date_filter = {'LeftOperand': from_filter,
                       'LogicalOperator': 'AND',
                       'RightOperand': to_filter}

        return date_filter

    def process_endpoint(self, endpoint_name_, endpoint_, filter_):
        out = []
        endpoint_ = endpoint_()
        endpoint_.auth_stub = self.client
        if not isinstance(filter_, type(None)):
            endpoint_.search_filter = filter_

        request = endpoint_.get()

        if not request.status:
            raise Exception(
                f"Request failed on endpoint {endpoint_name_}, message: "
                f"{request.message}")

        out = out + request.results
        while request.more_results:
            request = endpoint_.getMoreResults()
            out = out + request.results

        return out

    def get_events(self, filter_=None):
        file_name = SDCFileHelpers.get_file_path("schema",
                                                  "ExactTarget/events.json")
        json_data = json.loads(open(file_name).read())

        df = Dataframe(SDCDFTypes.PANDAS, json_data)
        data = []

        endpoints = {
            'SentEvent': FuelSDK.ET_SentEvent,
            'ClickEvent': FuelSDK.ET_ClickEvent,
            'OpenEvent': FuelSDK.ET_OpenEvent,
            'BounceEvent': FuelSDK.ET_BounceEvent,
            'UnsubEvent': FuelSDK.ET_UnsubEvent
        }

        for name, endpoint in endpoints.items():
            data = data + self.process_endpoint(name, endpoint, filter_)

        data = self.normalize_data(data)

        if len(data) >= 1:
            df.load_data(data)
            return df
        else:
            logging.warning("Received no data")
            return None

    def get_subscribers(self, filter_=None):
        file_name = SDCFileHelpers.get_file_path("schema",
                                                  "ExactTarget/subscribers.json")

        json_data = json.loads(open(file_name).read())

        df = Dataframe(SDCDFTypes.PANDAS, json_data)
        data = []

        data = self.process_endpoint("Subscriber", FuelSDK.ET_Subscriber,
                                     filter_)

        data = self.normalize_data(data)

        if len(data) >= 1:
            df.load_data(data)
            return df

        else:
            logging.warning("Received no data.")
            return None

    def get_list_subscribers(self, filter_=None):
        """
        This data has a Status property that can change over time. Will need to
        use this func twice to (1) grab new records for a day by CreatedDate
        and insert and (2) grab updated records for a day by ModifiedDate
        and upsert.

        :param filter_:
        :return:
        """
        file_name = SDCFileHelpers.get_file_path("schema",
                                                  "ExactTarget/list_subscribers.json")

        json_data = json.loads(open(file_name).read())

        df = Dataframe(SDCDFTypes.PANDAS, json_data)
        data = []

        data = self.process_endpoint("List_Subscriber",
                                     FuelSDK.ET_List_Subscriber,
                                     filter_)

        data = self.normalize_data(data)

        if len(data) >= 1:
            df.load_data(data)
            return df

        else:
            logging.warning("Received no data.")
            return None

    def get_sends(self, filter_=None):

        file_name = SDCFileHelpers.get_file_path("schema",
                                                  "ExactTarget/sends.json")

        json_data = json.loads(open(file_name).read())

        df = Dataframe(SDCDFTypes.PANDAS, json_data)
        data = []

        data = self.process_endpoint("Send", FuelSDK.ET_Send,
                                     filter_)

        data = self.normalize_data(data)

        if len(data) >= 1:
            df.load_data(data)
            return df

        else:
            logging.warning("Received no data.")
            return None
