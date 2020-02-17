import io
import os
from sdc_etl_libs.sdc_data_exchange.SDCDataExchangeEndpoint import \
    SDCDataExchangeEndpoint
from sdc_etl_libs.sdc_file_helpers.SDCFileFactory import SDCFileFactory
from sdc_etl_libs.sdc_dataframe.Dataframe import Dataframe
from sdc_etl_libs.sdc_file_helpers.SDCFile import SDCFile
from sdc_etl_libs.sdc_file_helpers.SDCCSVFile import SDCCSVFile
from sdc_etl_libs.sdc_file_helpers.SDCParquetFile import SDCParquetFile


#TODO: write base class for local disk that can search for files, etc..
class SDCLocalDiskEndpoint(SDCDataExchangeEndpoint):

    def __init__(self):

        super().__init__()
        self.endpoint_type = None
        self.data_schema = None
        self.endpoint_schema = None
        self.bucket = None
        self.prefix = None
        self.region = None
        self.file_regex = None
        self.decode = None
        self.file_type = None
        self.files = None
        self.file_name = None
        self.file_path = None
        self.df = None

    def create_exchange_endpoint(self, data_schema_, endpoint_schema_):
        """
        Creates a data exchange endpoint for local disk
        :param data_schema_: Dict. Entire JSON data schema.
        :param endpoint_schema_: Dict. JSON data schema of endpoint.
        :return: None.
        """

        self.endpoint_schema = endpoint_schema_
        self.data_schema = data_schema_

        self.endpoint_type = self.endpoint_schema["endpoint_type"]
        self.file_type = self.endpoint_schema["file_info"]["type"]
        self.file_name = self.endpoint_schema["file_info"]["file_name"]
        self.file_path = self.endpoint_schema["file_info"]["file_path"]
        self.file_regex = self.endpoint_schema["file_info"]["file_regex"]
        self.decode = self.endpoint_schema["file_info"]["decode"] if \
            self.endpoint_schema["file_info"]["decode"] else 'utf-8'

    def get_data(self, file_name_):
        """
        Returns data as a file like object.
        :param file_name_: Name of file to process.
        :return: filelike object object.
        """
        file_location = os.path.join(self.file_path, file_name_)
        out_file = open(file_location, "rb")

        out_file.seek(0)
        return out_file

    def write_data(self, data_, file_name_):
        if isinstance(data_, SDCFile) or isinstance(data_, SDCCSVFile) or isinstance(data_, SDCParquetFile):
            file_obj = data_.get_file_as_object()

        elif isinstance(data_, Dataframe):
            file_obj = data_.get_as_file_obj_from_endpoint_schema(self.endpoint_schema)


        if isinstance(file_obj, io.StringIO):
            with open(file_name_, 'w') as w:
                w.write(file_obj.getvalue())

        elif isinstance(file_obj, io.BytesIO):
            with open(file_name_, 'wb') as w:
                w.write(file_obj.getvalue())
        else:
            raise Exception("Local Disk Endpoint: Unknown file like object type")


