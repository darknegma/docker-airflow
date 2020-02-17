
import json
import logging
import requests
from typing import List
from sdc_etl_libs.api_helpers.API import API
from sdc_etl_libs.sdc_dataframe.Dataframe import Dataframe, SDCDFTypes
from sdc_etl_libs.sdc_file_helpers.SDCFileHelpers import SDCFileHelpers
from sdc_etl_libs.aws_helpers.aws_helpers import AWSHelpers
from airflow.models import Variable

logging.basicConfig(level=logging.INFO)

class TrueVault(API):

    def __init__(self):
        # secrets in the from of {"username":,"password":, "api_key": }
        #self.base_url = base_url_

        #self.credentials = self.get_credentials("aws_secrets", "truevault/api")
        self.credentials = AWSHelpers.get_secrets(
            'truevault/api',
            access_key_=Variable.get( "aws_authorization"),
            secret_key_=Variable.get("aws_secret_key"))
        self.auth = (self.credentials['username'], "")
        self.base_url = None
        self.vault_id = None
        self.schema_json = None

    def get_data_schema(self, schema_name_):
        """
        Grabs the data schema for TrueVault and returns to self.schema_json in
        JSON format.
        :param schema_name_: Name of data schema (without file extension)
        :return: None
        """

        file_name = SDCFileHelpers.get_file_path("schema", f'{schema_name_}.json')
        self.schema_json = json.loads(open(file_name).read())

        if "data_source" in self.schema_json \
                and self.schema_json["data_source"]["type"] == "api" \
                and "vault_id" in self.schema_json["data_source"]:
            self.base_url = self.schema_json["data_source"]["base_url"]
            self.vault_id = self.schema_json["data_source"]["vault_id"]
        else:
            raise Exception("Missing data_source metadata in schema definition.")

    def get_documents(self, document_key_list_: List[str], filter_=None):
        """
        This function will grab all the document keys provided in the list and return a dataframe.
        :param document_key_list_ The list of document keys to be looked up in truevault
        :param filter_: Specific filters to narrow down queried data.
        https://docs.truevault.com/documents#read-a-document
        :return: Dataframe
        """

        def chunks(l, n):
            """Yield successive n-sized chunks from l."""
            for i in range(0, len(l), n):
                yield ",".join(l[i:i + n])

        def get_data(url_, auth_):
            r = requests.get(url_, auth=auth_)
            out_data = []

            if r.status_code == 200:
                try:
                    data_json = json.loads(r.content)
                    for item in data_json["documents"]:
                        item["document"]["EXTERNAL_KEY"] = item["id"]
                        out_data.append(item["document"])
                except Exception as e:
                    logging.error(e)
                    raise Exception(f"Unable to process data: {r.content}")
            else:
                raise Exception(f"Error when calling endpoint {url_}")

            return out_data

        df = Dataframe(SDCDFTypes.PANDAS,self.schema_json)
        data = []

        for chunk in chunks(document_key_list_, 50):
            base_endpoint_url = self.base_url + f"/{self.vault_id}" + '/documents/' + chunk
            data += get_data(base_endpoint_url, self.auth)

        if len(data) >= 1:
            df.load_data(data)
            return df
        else:
            logging.warning("Recieved no data")
            return None

    def list_vaults(self):
        base_endpoint_url = self.base_url
        page_num = 1
        page_base_endpoint_url = base_endpoint_url + f"?page={page_num}"
        r = requests.get(page_base_endpoint_url, auth=self.auth)
        vault_ids = []
        if r.status_code == 200:
            try:
                data = json.loads(r.content)
                if "page" in data and "total" in data:
                    total_pages = data["total"]
                    vault_ids.extend(data["vaults"])
                    while page_num <= total_pages:
                        page_num += 1
                        page_base_endpoint_url = base_endpoint_url + \
                                                 f"?page={page_num}"
                        r = requests.get(page_base_endpoint_url, auth=self.auth)
                        if r.status_code == 200:
                            try:
                                data = json.loads(r.content)
                                vault_ids.extend(data["vaults"])
                            except Exception as e:
                                logging.error(e)
                                logging.error(r.content)
                                raise Exception(
                                    f"Unable to process data: {r.content}")
                else:
                    data = json.loads(r.content)
                    vault_ids.extend(data["vaults"])
            except Exception as e:
                logging.error(e)
                logging.error(r.content)
                raise Exception(f"Unable to process data: {r.content}")
        else:
            logging.error(r.content)
            raise Exception(
                f"Error when calling endpoint {page_base_endpoint_url}")

        return vault_ids

    def list_blobs(self):

        base_endpoint_url = self.base_url + f"/{self.vault_id}" + \
        '/blobs'
        page_num = 1
        page_base_endpoint_url = base_endpoint_url + f"?page={page_num}&per_page=1000"
        r = requests.get(page_base_endpoint_url, auth=self.auth)
        out_data = []
        logging.info(r)
        if r.status_code == 200:
            try:
                data = json.loads(r.content)
                if "page" in data["data"] and "total" in data["data"]:
                    total_pages = data["data"]["total"]
                    out_data.extend(data["data"]["items"])
                    per_page = data["data"]["per_page"]
                    logging.info(per_page)
                    logging.info(total_pages)
                    while page_num <= total_pages:

                        logging.info(page_num)
                        if page_num > 3:
                            break
                        page_num += 1
                        page_base_endpoint_url = base_endpoint_url + \
                                                 f"?page={page_num}&per_page=1000"
                        r = requests.get(page_base_endpoint_url, auth=self.auth)
                        if r.status_code == 200:
                            try:
                                data = json.loads(r.content)
                                out_data.extend(data["data"]["items"])
                            except Exception as e:
                                logging.error(e)
                                logging.error(r.content)
                                raise Exception(
                                    f"Unable to process data: {r.content}")
                        total_pages = data["data"]["total"]
                        out_data.extend(data["data"]["items"])
                        logging.info(per_page)
                        logging.info(total_pages)
                else:
                    data = json.loads(r.content)
                    out_data.extend(data["data"]["items"])
            except Exception as e:
                logging.error(e)
                logging.error(r.content)
                raise Exception(f"Unable to process data: {r.content}")
        else:
            logging.error(r.content)
            raise Exception(
                f"Error when calling endpoint {page_base_endpoint_url}")

        return out_data

    def list_documents(self):

        base_endpoint_url = self.base_url + f"/{self.vault_id}" + \
        '/documents'
        page_num = 1
        page_base_endpoint_url = base_endpoint_url + f"?page={page_num}&per_page=1000"
        r = requests.get(page_base_endpoint_url, auth=self.auth)
        out_data = []
        logging.info(r)
        if r.status_code == 200:
            try:
                data = json.loads(r.content)
                if "page" in data["data"] and "total" in data["data"]:
                    total_pages = data["data"]["total"]
                    out_data.extend(data["data"]["items"])
                    per_page = data["data"]["per_page"]
                    logging.info(per_page)
                    logging.info(total_pages)
                    while page_num <= total_pages:

                        logging.info(page_num)
                        if page_num > 3:
                            break
                        page_num += 1
                        page_base_endpoint_url = base_endpoint_url + \
                                                 f"?page={page_num}&per_page=1000"
                        r = requests.get(page_base_endpoint_url, auth=self.auth)
                        if r.status_code == 200:
                            try:
                                data = json.loads(r.content)
                                out_data.extend(data["data"]["items"])
                            except Exception as e:
                                logging.error(e)
                                logging.error(r.content)
                                raise Exception(
                                    f"Unable to process data: {r.content}")
                        total_pages = data["data"]["total"]
                        out_data.extend(data["data"]["items"])
                        logging.info(per_page)
                        logging.info(total_pages)
                else:
                    data = json.loads(r.content)
                    out_data.extend(data["data"]["items"])
            except Exception as e:
                logging.error(e)
                logging.error(r.content)
                raise Exception(f"Unable to process data: {r.content}")
        else:
            logging.error(r.content)
            raise Exception(
                f"Error when calling endpoint {page_base_endpoint_url}")

        return out_data
