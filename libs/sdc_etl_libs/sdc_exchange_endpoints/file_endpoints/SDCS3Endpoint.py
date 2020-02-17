
from sdc_etl_libs.sdc_data_exchange.SDCDataExchangeEndpoint import SDCDataExchangeEndpoint
from sdc_etl_libs.sdc_s3.SDCS3 import SDCS3
from sdc_etl_libs.aws_helpers.aws_helpers import AWSHelpers
from sdc_etl_libs.sdc_file_helpers.SDCFile import SDCFile
from sdc_etl_libs.sdc_file_helpers.SDCFileFactory import SDCFileFactory
from sdc_etl_libs.sdc_dataframe.Dataframe import Dataframe


class SDCS3Endpoint(SDCS3, SDCDataExchangeEndpoint):

    def __init__(self):

        super().__init__()
        self.endpoint_type = None
        self.exchange_type = "s3"
        self.endpoint_tag = None
        self.data_schema = None
        self.endpoint_schema = None
        self.access_key = None
        self.secret_key = None
        self.bucket = None
        self.prefix = None
        self.region = None
        self.file_regex = None
        self.decode = None
        self.file_type = None
        self.files = None
        self.df = None

    def create_exchange_endpoint(self, data_schema_, endpoint_schema_):
        """
        Creates a data exchange endpoint for S3. Establishes connection
        to S3. A list of files already that exist in the S3 path are returned
        to self.files.
        :param data_schema_: Dict. Entire JSON data schema.
        :param endpoint_schema_: Dict. JSON data schema of endpoint.
        :return: None.
        """

        self.endpoint_schema = endpoint_schema_
        self.data_schema = data_schema_
        self.endpoint_tag = self.endpoint_schema["tag"]

        self.endpoint_type = self.endpoint_schema["endpoint_type"]
        self.file_type = self.endpoint_schema["file_info"]["type"]
        self.file_regex = self.endpoint_schema["file_info"]["file_regex"]
        self.decode = self.endpoint_schema["file_info"]["decode"] if \
            self.endpoint_schema["file_info"]["decode"] else 'utf-8'

        self.bucket = self.endpoint_schema["bucket"]
        self.prefix = self.endpoint_schema["prefix"] if \
            self.endpoint_schema["prefix"] else ''
        self.region = self.endpoint_schema["region"]

        access_key = None
        secret_key = None

        if "credentials" in self.endpoint_schema:
            if self.endpoint_schema["credentials"]:
                if self.endpoint_schema["credentials"]["type"] == "awssecrets":
                    creds = AWSHelpers.get_secrets(
                        self.endpoint_schema["credentials"]["name"])
                    access_key = creds["access_key"]
                    secret_key = creds["secret_key"]

        self.connect(
            access_key_=access_key,
            secret_key_=secret_key,
            region_=self.region)

        self.files = self.get_obj_list(
            bucket_name_=self.bucket,
            prefix_=self.prefix,
            obj_regex_=self.file_regex,
            give_full_path_=False)

    def get_data(self, file_name_):
        """
        Returns data from file as SDCDataframe object with dataframe.
        :param file_name_: Name of file to process.
        :return: SDCDataframe object.
        """

        file_obj = self.get_file_as_file_object(
            bucket_name_=self.bucket,
            prefix_=self.prefix,
            file_name_=file_name_,
            decode_=self.decode)

        sdc_file = SDCFileFactory.get_file(
            schema_=self.data_schema,
            endpoint_schema_=self.endpoint_schema,
            file_name_=file_name_,
            file_path_=self.bucket + self.prefix,
            file_obj_=file_obj)

        return sdc_file

    def write_data(self, data_, file_name_):
        """
        Writes a SDCDataframe or SDCFile object out to a file on S3.
        :param file_name_: Name to write file as.
        :param data_: Data to be written to SFTP. Can be SDCDataframe object or
            SDCFile object.
        :return: Log results of writing file to SFTP site as string.
        """
        result = None
        if isinstance(data_, SDCFile):
            file_obj = SDCFileFactory.get_endpoint_file_obj(self.endpoint_schema, data_)
            result = self.write_file(
                self.bucket, self.prefix, file_name_, file_obj)

        elif isinstance(data_, Dataframe):
            file_obj = data_.get_as_file_obj_from_endpoint_schema(
                self.endpoint_schema)
            result = self.write_file(
                self.bucket, self.prefix, file_name_, file_obj)

        return result
