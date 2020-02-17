from sdc_etl_libs.sdc_dataframe.Dataframe import Dataframe, SDCDFTypes
from sdc_etl_libs.sdc_file_helpers.SDCFileHelpers import SDCFileHelpers
from sdc_etl_libs.sdc_file_helpers.SDCFile import SDCFile

import pandas as pd
import json


class Experian(SDCFile):
    def __init__(self):

        file_search = "Vendors/Experian/experian_metadata_usa.xlsx"

        meta_data_filename = SDCFileHelpers.get_file_path("metadata",
                                                          file_search)

        self.metadata_df = pd.read_excel(meta_data_filename)
        self.header = self.metadata_df.iloc[0]
        self.metadata_df = self.metadata_df.drop(self.metadata_df.index[0])
        self.metadata_list = list(
            self.metadata_df.apply(Experian.generate_metadata, axis=1))

    @staticmethod
    def generate_metadata(row_):
        metadata = {
            "column_name": row_[1],
            "field_id": row_[0],
            "start": row_[3] - 1,
            "end": row_[4]
        }
        return metadata

    def process_row(self, row_, metadata_):
        out_data = {}
        for column in metadata_:
            # out_data["field_id"] = column["field_id"]
            out_data[column["column_name"]] = row_[column["start"]:column["end"]]
        return out_data

    def process_file(self, file_obj_):
        data = []
        file_obj_.seek(0)
        file_name = SDCFileHelpers.get_file_path("schema",
                                                 "Experian/experian.json")
        json_data = json.loads(open(file_name).read())
        df = Dataframe(SDCDFTypes.PANDAS, json_data)

        for row in file_obj_:
            data.append(
                self.process_row(row.lstrip().rstrip(), self.metadata_list))

        df.load_data(data)
        return df
