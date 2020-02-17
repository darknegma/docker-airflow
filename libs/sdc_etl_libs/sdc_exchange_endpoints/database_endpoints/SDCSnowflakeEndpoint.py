
from sdc_etl_libs.sdc_data_exchange.SDCDataExchangeEndpoint import SDCDataExchangeEndpoint
from sdc_etl_libs.database_helpers.SnowflakeDatabase import SnowflakeDatabase
from sdc_etl_libs.aws_helpers.aws_helpers import AWSHelpers
from sdc_etl_libs.sdc_file_helpers.SDCFile import SDCFile
from sdc_etl_libs.sdc_file_helpers.SDCCSVFile import SDCCSVFile
from sdc_etl_libs.sdc_file_helpers.SDCParquetFile import SDCParquetFile
from sdc_etl_libs.sdc_file_helpers.SDCFileHelpers import SDCFileHelpers
import pandas as pd
import logging
from sdc_etl_libs.sdc_dataframe.Dataframe import SDCDFTypes,Dataframe


class DataframeForDBEmpty(Exception):
    def __init__(self, value):
        Exception.__init__(self, value)

class DataframeFailedLoading(Exception):
    def __init__(self, value):
        Exception.__init__(self, value)


class SDCSnowflakeEndpoint(SnowflakeDatabase, SDCDataExchangeEndpoint):

    def __init__(self):
        """
        Snowflake endpoint constructor.
        """

        super().__init__()
        self.endpoint_type = None
        self.exchange_type = "snowflake"
        self.endpoint_tag = None
        self.data_schema = None
        self.endpoint_schema = None
        self.access_key = None
        self.secret_key = None
        self.table_name = None
        self.database_schema = None
        self.database = None
        self.write_filename_to_db = None
        self.username = None
        self.account = None
        self.warehouse = None
        self.role = None
        self.files = []
        self.query = None
        self.upsert = None
        self.dedupe = None
        self.bookmark_filenames = None
        self.sqlalchemy = True
        self.sql_file_path = None

    def create_exchange_endpoint(self, data_schema_, endpoint_schema_):
        """
        Creates a data exchange endpoint for Snowflake. Establishes connection
        to Snowflake. If endpoint is a sink, a list of files already loaded
        to table is set to self.files. If endpoint is a source, a source query
        is set to self.query
        :param data_schema_: Dict. Entire JSON data schema.
        :param endpoint_schema_: Dict. JSON data schema of endpoint.
        :return: None.
        """

        self.endpoint_schema = endpoint_schema_
        self.data_schema = data_schema_
        self.endpoint_tag = self.endpoint_schema["tag"]

        self.endpoint_type = self.endpoint_schema["endpoint_type"]
        self.table_name = self.endpoint_schema["table_name"]
        self.database_schema = self.endpoint_schema["schema"]
        self.database = self.endpoint_schema["database"]
        self.write_filename_to_db = self.endpoint_schema["write_filename_to_db"]
        self.upsert = self.endpoint_schema["upsert"] if \
            self.endpoint_schema["upsert"] else False
        self.dedupe = self.endpoint_schema["dedupe"] if \
            self.endpoint_schema["dedupe"] else False
        self.bookmark_filenames = self.endpoint_schema["bookmark_filenames"] if \
            self.endpoint_schema["bookmark_filenames"] else False

        if self.endpoint_schema["credentials"]["type"] == "awssecrets":
            creds = AWSHelpers.get_secrets(self.endpoint_schema["credentials"]["name"])
            self.username = creds["username"]
            self.account = creds["account"]
            self.warehouse = creds["warehouse"]
            self.role = creds["role"]
            password = creds["password"]

        self.connect(
            user_=self.username,
            password_=password,
            account_=self.account,
            warehouse_=self.warehouse,
            role_=self.role,
            database_=self.database,
            schema_=self.database_schema)

        if self.bookmark_filenames:
            if self.endpoint_type == 'sink':
                self.files = self.get_file_bookmarks(
                    database_=self.database,
                    schema_=self.database_schema,
                    for_table_=self.table_name)

            elif self.endpoint_type == 'source':
                self.sql_file_path = self.endpoint_schema["sql_file_path"]
                try:
                    file_path = SDCFileHelpers.get_file_path("sql", self.sql_file_path)
                    with open(file_path, 'r') as file:
                        self.query = file.read()
                except Exception:
                    raise Exception("Error reading snowflake sql file ")

    def get_data(self):
        #TODO: Need to revisit once we integrate spark.
        try:
            df = pd.read_sql(self.query, self.connection)
        except Exception as e:
            logging.exception(e)
            raise Exception("Error loading Snowflake data into Pandas occured while executing query")
        sdc_df = Dataframe(SDCDFTypes.PANDAS, self.data_schema)
        sdc_df.process_df(df)
        return sdc_df

    def write_data(self, data_, file_name_=None):
        """
        Write out a SDCDataframe or SDCFile object to a Snowflake table.
        :param file_name_: Name of file.
        :param data_: Data to be written to SFTP. Can be SDCDataframe object or
            SDCFile object.
        :return: Log results of writing data to Snowflake.
        """

        df = None

        if isinstance(data_, SDCFile) or isinstance(data_, SDCCSVFile) \
                or isinstance(data_, SDCParquetFile):
            try:
                df = data_.get_file_as_dataframe()
            except Exception:
                logging.exception(f"{file_name_} failed loading to dataframe.")

        elif isinstance(data_, Dataframe):
            df = data_

        else:
            raise Exception("SDC Snowflake received an unknown data type.")

        data_length = len(df.df)

        if data_length == 0:
            raise DataframeForDBEmpty(f"{file_name_} was empty.")

        if self.write_filename_to_db:
            if file_name_:
                df.fill_in_column(column_name_='_ETL_FILENAME',
                                  column_value_=file_name_,
                                  create_column_=True)
            else:
                raise Exception(
                    "No file name was given to write to the Snowflake table.")

        try:
            df.cleanse_column_names("snowflake")
            result = df.write_dataframe_to_database(
                sdc_database_handle_=self,
                table_name_=self.table_name,
                schema_name_=self.database_schema,
                upsert_=self.upsert,
                dedupe_=self.dedupe
            )
        except Exception as e:
            logging.info(e)
            raise Exception(f"Error loading data into Snowflake.")

        if self.bookmark_filenames:
            if file_name_:
                try:
                    self.update_file_bookmarks(
                        database_=self.database,
                        schema_=self.database_schema,
                        for_table_=self.table_name.upper(),
                        file_name_=file_name_)
                except Exception as e:
                    raise Exception("Error updating bookmarking table.")
            else:
                raise Exception(
                    "No file name was given to write to the Snowflake table.")
        else:
            pass

        return result
