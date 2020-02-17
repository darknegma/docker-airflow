
import logging
from sdc_etl_libs.sdc_file_helpers.SDCCSVFile import SDCCSVFile
from sdc_etl_libs.sdc_file_helpers.SDCFile import SDCFile
from sdc_etl_libs.sdc_file_helpers.SDCParquetFile import SDCParquetFile


class SDCFileFactory(object):

    @staticmethod
    def get_file(schema_, endpoint_schema_, file_name_, file_path_, file_obj_):
        """
        This function is used to create an sdcfile when reading a raw datafile from a source.

        :param schema_: Json schema of the data.
        :param endpoint_schema_: The endpoint schema of the source
        :param file_name_: Name of the file
        :param file_path_: Path you wish to write too
        :param file_obj_: Raw file object
        :return: SDCFILE
        """

        file_type = endpoint_schema_["file_info"]["type"]

        if file_type.lower() == "csv":
            return SDCCSVFile(schema_, endpoint_schema_, file_name_, file_path_,
                              file_obj_)
        elif file_type.lower() == "parquet":
            return SDCParquetFile(schema_, endpoint_schema_, file_name_, file_path_, file_obj_)
        elif file_type.lower() == "file":
            return SDCFile(schema_, endpoint_schema_, file_name_, file_path_, file_obj_)
        else:
            logging.exception(f"{file_type} is not a valid file option.")

    @staticmethod
    def get_endpoint_file_obj(endpoint_schema_, sdcfile_obj_):
        """
        This method takes a endpoint schema and sdcfile and outputs the file
        in accordance to the endpoint file_info parameters

        :param endpoint_schema_: The json schema of the endpoint
        :param sdcfile_obj_: A SDCFILE object with data loaded in it.
        :return: File like object (BytesIO)
        """

        if not isinstance(sdcfile_obj_, SDCFile):
            raise Exception("Must pass in a SDCFILE object type")


        file_type = endpoint_schema_["file_info"]["type"]

        if file_type == "csv" or file_type == "parquet":
            if isinstance(sdcfile_obj_, SDCParquetFile) or isinstance(sdcfile_obj_, SDCCSVFile):
                temp_df = sdcfile_obj_.get_file_as_dataframe()
                return temp_df.get_as_file_obj_from_endpoint_schema(endpoint_schema_)
            else:
                raise Exception(f"Unable to produce output file {file_type} from raw file.")

        elif file_type == "file":
            return sdcfile_obj_.get_file_as_object()
        else:
            logging.exception(f"{file_type} is not a valid file option.")
