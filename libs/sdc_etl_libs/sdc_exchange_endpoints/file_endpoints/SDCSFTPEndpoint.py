
import os
from sdc_etl_libs.sdc_data_exchange.SDCDataExchangeEndpoint import SDCDataExchangeEndpoint
from sdc_etl_libs.sdc_filetransfer.SFTPFileTransfer import SFTP
from sdc_etl_libs.aws_helpers.aws_helpers import AWSHelpers
from sdc_etl_libs.sdc_dataframe.Dataframe import Dataframe
from sdc_etl_libs.sdc_file_helpers.SDCFile import SDCFile
from sdc_etl_libs.sdc_file_helpers.SDCFileFactory import SDCFileFactory


class SDCSFTPEndpoint(SFTP, SDCDataExchangeEndpoint):

    def __init__(self):
        """
        SFTP class constructor.
        """

        super().__init__()
        self.exchange_type = "sftp"
        self.endpoint_type = None
        self.endpoint_tag = None
        self.data_schema = None
        self.endpoint_schema = None
        self.file_type = None
        self.file_regex = None
        self.host = None
        self.port = None
        self.path = None
        self.files = None

    def create_exchange_endpoint(self, data_schema_, endpoint_schema_):
        """
        Creates a data exchange endpoint for SFTP. Establishes connection
        to SFTP. A list of files already that exist in the SFTP path are returned
        to self.files.
        :param data_schema_: Dict. Entire JSON data schema.
        :param endpoint_schema_: Dict. JSON data schema of endpoint.
        :return: None.
        """

        self.endpoint_schema = endpoint_schema_
        self.data_schema = data_schema_
        self.endpoint_tag = self.endpoint_schema["tag"]

        self.endpoint_type = self.endpoint_schema["endpoint_type"]
        self.file_regex = self.endpoint_schema["file_info"]["file_regex"]

        self.host = self.endpoint_schema["host"]
        self.port = self.endpoint_schema["port"]
        self.path = self.endpoint_schema["path"]

        if self.endpoint_schema["credentials"]["type"] == "awssecrets":
            creds = AWSHelpers.get_secrets(
                self.endpoint_schema["credentials"]["name"])
            username = creds["username"]
            password = None
            pkey = None
            if "password" in creds and creds["password"]:
                password = creds["password"]
            elif "rsa_key" in creds and creds["rsa_key"]:
                pkey = creds["rsa_key"].replace('\\n', '\n')

        else:
            raise Exception("No method for passing credentials given.")

        self.connect(
            host_=self.host,
            username_=username,
            password_=password,
            rsa_key_=pkey,
            port_=self.port)

        self.files = self.get_obj_list(
            path_=self.path,
            obj_regex_=self.file_regex,
            give_full_path_=False,
            include_dirs_=True)

    def get_data(self, file_name_):
        """
        Returns data from file as SDCDataframe object with dataframe.
        :param file_name_: Name of file to process.
        :return: SDCDataframe object.
        """

        file_obj = self.get_file_as_file_object(
            os.path.join(self.path, file_name_)
        )

        sdc_file = SDCFileFactory.get_file(
            schema_=self.data_schema,
            endpoint_schema_=self.endpoint_schema,
            file_name_=file_name_,
            file_path_=self.path,
            file_obj_=file_obj)

        return sdc_file

    def write_data(self, data_, file_name_):
        """
        Writes a SDCDataframe or SDCFile object out to a file on SFTP.
        :param file_name_: Name to write file as.
        :param data_: Data to be written to SFTP. Can be SDCDataframe object or
            SDCFile object.
        :return: Log results of writing file to SFTP site as string.
        """

        if isinstance(data_, SDCFile):
            file_obj = SDCFileFactory.get_endpoint_file_obj(self.endpoint_schema, data_)
            result = self.write_file(
                self.path, file_name_, file_obj)

        elif isinstance(data_, Dataframe):
            file_obj = data_.get_as_file_obj_from_endpoint_schema(
                self.endpoint_schema)
            result = self.write_file(self.path, file_name_, file_obj)

        return result
