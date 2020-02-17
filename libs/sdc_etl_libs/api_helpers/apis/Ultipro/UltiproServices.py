
import datetime
import decimal
import json
import logging
import zeep
from sdc_etl_libs.api_helpers.apis.Ultipro.Ultipro import Ultipro
from sdc_etl_libs.sdc_dataframe.Dataframe import Dataframe, SDCDFTypes
from sdc_etl_libs.sdc_file_helpers.SDCFileHelpers import SDCFileHelpers

logging.basicConfig(level=logging.INFO)

class UltiproServices(Ultipro):

    def __init__(self):
        super().__init__()

    def convert_to_literals(self, dict_):
        """
        Recursively travels a data set dictionary - at both the dictionary
        and list level - and converts Decimal() objects to literal floats and
        datetime.datetimes objects to literal datetime strings.

        :param dict_: Dictionary to evaluate.
        :return: None
        """

        for key, value in dict_.items():

            if isinstance(value, dict):
                self.convert_to_literals(value)

            elif isinstance(value, list):
                for num, item in enumerate(value):
                    if isinstance(item, dict):
                        self.convert_to_literals(item)
            else:
                if isinstance(value, decimal.Decimal):
                    dict_[key] = float(value)
                elif isinstance(value, datetime.datetime):
                    dict_[key] = value.strftime('%Y-%m-%d %H:%M:%S')

    def get_service(self, service_name_):
        """
        Given an endpoint, creaetes the Zeep client, authenticates and returns
        the service function for use.

        :param service_name_: Ultipro SOAP service name.
        :return: Zeep client service for Ultipro endpoint.
        """

        if service_name_ == 'FindCompensations':
            endpoint = 'EmployeeCompensation'
            zeep_client = zeep.Client(f"{self.base_url}{endpoint}")
            service = zeep_client.service.FindCompensations

        elif service_name_ == 'EmployeeEmploymentInformation':
            endpoint = "EmployeeEmploymentInformation"
            zeep_client = zeep.Client(f"{self.base_url}{endpoint}")
            service = zeep_client.service.FindEmploymentInformations

        elif service_name_ == 'FindLastPayStatement':
            endpoint = "EmployeePayStatement"
            zeep_client = zeep.Client(f"{self.base_url}{endpoint}")
            service = zeep_client.service.FindLastPayStatement

        else:
            raise Exception(f"{service_name_} is no currently supported.")

        self.soap_authenticate()

        return service, endpoint, zeep_client

    def process_endpoint(self, service_name_, query_=None, limit_=None):
        """
        Processes an Ultipro SOAP endpoint.

        :param service_name_: Ultipro SOAP service name.
        :param query_: SOAP query as a dictionary.
        :param limit_: Page results limit.
        :return: Results as Zeep array object
        """

        results = []

        if query_ == None:
            query_ = {}
        elif query_ and not isinstance(query_, dict):
            raise Exception("Query for Ultipro SOAP must be in dictionary format.")

        # Paginate by updating the query in with a new PageNumber. The current_page
        # and total_pages are evaluated to determine if we got every page. To start,
        # set current_page to 1 and total_pages to 2 (to ensure the first call is made).
        # The first call will update to the correct total_pages.
        query_["PageNumber"] = "1"
        query_["PageSize"] = limit_
        current_page = 1
        total_pages = 2

        service, endpoint, zeep_client = self.get_service(service_name_)

        while current_page < total_pages:

            response = service(_soapheaders=[self.session_header], query=query_)

            if response["OperationResult"]["Success"] == True:

                total_pages = int(
                    response["OperationResult"]["PagingInfo"]["PageTotal"])
                current_page = int(
                    response["OperationResult"]["PagingInfo"]["CurrentPage"])
                records = response["Results"][endpoint]

                if current_page == 1:
                    logging.info(f"Total pages: {total_pages}.")

                results.extend(records)
                logging.info(f'Grabbed {query_["PageSize"]} record(s) from Page #{current_page}.')

                query_["PageNumber"] = \
                    str(int(response["OperationResult"]["PagingInfo"][
                                "CurrentPage"]) + 1)

            else:
                msg = response["OperationResult"]["Messages"]
                raise Exception(f"Grabbing from {endpoint} failed. {msg}")

        return results

    def convert_zeep_result_to_dict(self, data_):
        """
        Converts a Ultipro Services Zeep array object to a list of flattened
        dictionaries.

        :param data_: Ultipro Services Zeep array to process.
        :return: List of flattened dictionaries.
        """

        data = []
        for result in data_:
            empId = dict(zeep.helpers.serialize_object(result, target_cls=dict))
            empInfo = dict(empId.popitem()[1]).popitem()[1]
            for item in empInfo:
                item.update(empId)
                data.append(item)

        return data

    def get_employee_employement_information(self, query_=None, limit_=500):
        """
        Processes Ultipro SOAP Employee Employment Information endpoint results.
        https://connect.ultipro.com/documentation#/api/1168

        :param query_: SOAP query as a dictionary.
        :param limit_: Page results limit.
        :return: SDCDataFrame object with data in dataframe.
        """

        file_name = SDCFileHelpers.get_file_path(
            "schema", "Ultipro/services/employee-employment-information.json")
        json_data = json.loads(open(file_name).read())
        if "data_source" in json_data and json_data["data_source"][
            "type"] == "api": self.base_url = json_data["data_source"]["base_url"]
        else:
            raise Exception("Missing data_source metadata in schema definition.")

        df = Dataframe(SDCDFTypes.PANDAS, json_data)

        results = self.process_endpoint('EmployeeEmploymentInformation',
                                        query_=query_, limit_=limit_)

        data = self.convert_zeep_result_to_dict(results)

        if len(data) >= 1:
            df.load_data(data)
            return df
        else:
            logging.warning("Received no data")
            return None

    def get_employee_compensation(self, query_=None, limit_=500):
        """
        Processes Ultipro SOAP Employee Compensation endpoint results.
        https://connect.ultipro.com/documentation#/api/1144

        :param query_: SOAP query as a dictionary.
        :param limit_: Page results limit.
        :return: SDCDataFrame object with data in dataframe.
        """

        file_name = SDCFileHelpers.get_file_path(
            "schema", "Ultipro/services/employee-compensation.json")
        json_data = json.loads(open(file_name).read())
        if "data_source" in json_data and json_data["data_source"][
            "type"] == "api":
            self.base_url = json_data["data_source"]["base_url"]
        else:
            raise Exception("Missing data_source metadata in schema definition.")

        df = Dataframe(SDCDFTypes.PANDAS, json_data)

        results = self.process_endpoint('FindCompensations', query_=query_,
                                        limit_=limit_)

        data = self.convert_zeep_result_to_dict(results)

        if len(data) >= 1:
            df.load_data(data)
            return df
        else:
            logging.warning("Received no data")
            return None

    def get_employee_latest_pay_statement(self, query_=None, limit_=1000):
        """
        Processes Ultipro SOAP Employee Pay Statement endpoint results. Pulls
        the most recent pay statement for each Employee in system.
        https://connect.ultipro.com/documentation#/api/1150

        :param query_: SOAP query as a dictionary.
        :param limit_: Page results limit.
        :return: SDCDataFrame object with data in dataframe.
        """

        file_name = SDCFileHelpers.get_file_path(
            "schema", "Ultipro/services/employee-pay-statement.json")
        json_data = json.loads(open(file_name).read())
        if "data_source" in json_data and json_data["data_source"]["type"] == "api":
            self.base_url = json_data["data_source"]["base_url"]
        else:
            raise Exception("Missing data_source metadata in schema definition.")

        df = Dataframe(SDCDFTypes.PANDAS, json_data)

        results = self.process_endpoint('FindLastPayStatement', query_=query_,
                                        limit_=limit_)

        data = self.convert_zeep_result_to_dict(results)

        for i in data:
            self.convert_to_literals(i)

        if len(data) >= 1:
            df.load_data(data)
            df.drop_columns(["SSN"])
            return df
        else:
            logging.warning("Received no data")
            return None
