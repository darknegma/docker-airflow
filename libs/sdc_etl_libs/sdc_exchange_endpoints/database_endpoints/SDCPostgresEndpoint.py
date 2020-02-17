
from sdc_etl_libs.sdc_data_exchange.SDCDataExchangeEndpoint import SDCDataExchangeEndpoint
from sdc_etl_libs.database_helpers.PostgresSqlDatabase import PostgresSqlDatabase
from sdc_etl_libs.aws_helpers.aws_helpers import AWSHelpers
from sdc_etl_libs.sdc_file_helpers.SDCCSVFile import SDCCSVFile
from sdc_etl_libs.sdc_file_helpers.SDCParquetFile import SDCParquetFile
import logging


class DataframeForDBEmpty(Exception):
    def __init__(self, value):
        Exception.__init__(self, value)

class DataframeFailedLoading(Exception):
    def __init__(self, value):
        Exception.__init__(self, value)


class SDCPostgresEndpoint(PostgresSqlDatabase, SDCDataExchangeEndpoint):

    def __init__(self):
        """
        Postgres endpoint constructor.
        """

        super().__init__()
        self.endpoint_type = None
        self.exchange_type = "postgres"
        self.endpoint_tag = None
        self.data_schema = None
        self.endpoint_schema = None
        self.table_name = None
        self.database_schema = None
        self.database = None
        self.write_filename_to_db = None
        self.username = None
        self.files = []
        self.server = None
        self.port = 5432
        self.query = None
        self.upsert = None
        self.dedupe = None
        self.bookmark_filenames = None
        self.sqlalchemy = True

    def create_exchange_endpoint(self, data_schema_, endpoint_schema_):
        """
        Creates a data exchange endpoint for Postgres. Establishes connection
        to Postgres. If endpoint is a sink, a list of files already loaded
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
            self.server = creds["server"]
            self.port = creds["port"]
            password = creds["password"]

        self.connect(
            server_=self.server,
            database_=self.database,
            username_=self.username,
            password_=password,
            port_=self.port)



    def get_data(self):
        #TODO: Need code for get database as a source.
        pass

    def write_data(self, data_, file_name_=None):
        """
        Write out a SDCDataframe or SDCFile object to a Postgres table.
        :param file_name_: Name of file.
        :param data_: Data to be written to SFTP. Can be SDCDataframe object or
            SDCFile object.
        :return: Log results of writing data to Postgres.
        """

        df = None

        if isinstance(data_, SDCCSVFile) or isinstance(data_, SDCParquetFile):
            try:
                df = data_.get_file_as_dataframe()
            except Exception:
                logging.exception(f"{file_name_} failed loading to dataframe.")

        elif isinstance(data_, Dataframe):
            df = data_

        else:
            raise Exception("SDC Postgres received an Unsupported data type.")

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
                    "No file name was given to write to the postgres table.")

        try:
            df.cleanse_column_names("postgres")
            result = df.write_dataframe_to_database(
                sdc_database_handle_=self,
                table_name_=self.table_name,
                schema_name_=self.database_schema,
                upsert_=self.upsert,
                dedupe_=self.dedupe
            )
        except Exception as e:
            raise Exception(f"Error loading data into Postgres.")

        if self.bookmark_filenames:
            if file_name_:
                raise Exception("Bookmarks currently not supported with potgressql")
            else:
                raise Exception("Bookmarks currently not supported with potgressql")
        else:
            pass


        return result
