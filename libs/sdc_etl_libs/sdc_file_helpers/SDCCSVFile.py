
import csv
import numpy as np
import pandas as pd
from sdc_etl_libs.sdc_dataframe.Dataframe import Dataframe, SDCDFTypes
from sdc_etl_libs.sdc_file_helpers.SDCFile import SDCFile
from sdc_etl_libs.sdc_data_schema.SDCDataSchema import SDCDataSchema
import logging


#TODO: ADD checks to figure out what reader and dataframe we are using.
#When we have the autoscaling feature, the dataframe type used will dictate
#what options to use.
class SDCCSVFile(SDCFile):
    type = None
    file_name = None
    file_path = None
    file_obj = None
    schema = None

    def __init__(self, schema_, endpoint_schema_, file_name_, file_path_,
                 file_obj_):

        self.schema = schema_
        self.file_name = file_name_
        self.file_path = file_path_
        self.file_obj = file_obj_
        self.endpoint_schema = endpoint_schema_
        self.args = {}

        self.args = SDCDataSchema.generate_file_output_args(self.schema, self.endpoint_schema)
        if self.args is None or self.args == {}:
            raise Exception("Missing CSV Args")

        self.__process_args()

    def __process_args(self):
        #TODO: add check for different types of dataframes.
        if "header" in self.args:
            if self.args["header"]:
                self.args["header"] = 0
            else:
                self.args["header"] = None


    def get_file_size(self):
        pass

    def get_file_as_object(self):
        """
        Creates SDCDataframe from file data and returns an in-memory, file-like object.
        :return: Data from file as StringIO object.
        """
        # read into data frame
        df = self.get_file_as_dataframe()

        # use dataframe to create csv file object
        out_file = df.write_to_csv(**self.args)

        return out_file

    def get_file_as_dataframe(self):
        """
        Converts a csv file object to a dataframe.
        :return: A fully processed SDCDataframe.
        """

        data = []
        df = Dataframe(SDCDFTypes.PANDAS, self.schema)

        try:
            self.file_obj.seek(0)
            pandas_df = pd.read_csv(self.file_obj, index_col=False, dtype=np.object_, **self.args)
            df.process_df(pandas_df)

        except Exception as e:
            logging.error(e)
            logging.error(f"Failed loading csv to dataframe")

        return df
