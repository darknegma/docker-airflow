
import logging
from sdc_etl_libs.sdc_dataframe.Dataframe import Dataframe, SDCDFTypes
from sdc_etl_libs.sdc_file_helpers.SDCFile import SDCFile


class SDCParquetFile(SDCFile):
    type = None
    file_name = None
    file_path = None
    file_obj = None
    schema = None
    endpoint_schema = None
    df = None
    endpoint_type = None

    def __init__(self, schema_, endpoint_schema_, file_name_, file_path_,
                 file_obj_):

        self.schema = schema_
        self.file_name = file_name_
        self.file_path = file_path_
        self.file_obj = file_obj_
        self.endpoint_schema = endpoint_schema_

    def get_file_size(self):
        pass

    def get_file_as_object(self):
        """
        Writes out the contents of the dataframe into Parquet File like object.
        :return: File Like object (BufferIO)
        """
        df = self.get_file_as_dataframe()
        output = df.write_to_parquet()

        return output

    def get_file_as_dataframe(self):
        """
        Converts a parquet file object to a dataframe.

        :return: A fully processed SDCDataframe.
        """

        df = Dataframe(SDCDFTypes.PANDAS, self.schema)
        try:
            self.file_obj.seek(0)
            df.read_from_parquet(self.file_obj)
        except Exception as e:
            logging.error(e)
            logging.error(f"Failed processing parquet file.")

        return df
