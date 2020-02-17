
import ast
import logging
import pandas as pd
import numpy as np
import json
from enum import Enum
import uuid
import io
from sdc_etl_libs.database_helpers.Database import SDCDBTypes
from sdc_etl_libs.sdc_data_schema.SDCDataSchema import SDCDataSchema
from sdc_etl_libs.database_helpers.SnowflakeDatabase import SnowflakeDatabase


class SDCDFTypes(Enum):
    PANDAS = "pandas"
    SPARK = "spark"


class SDCDFPandasTypes(Enum):
    string = np.object_
    boolean = np.bool_
    float = np.float32
    double = np.float64
    int = pd.Int32Dtype()
    long = pd.Int64Dtype()
    datetime = np.datetime64
    null = pd.np.nan
    json = np.object_


# List of dataframe types that should not be double-quoted when generating
# SQL queries (as they are not string values).
SDCDFNonQuotableTypes = [
    SDCDFPandasTypes.boolean,
    SDCDFPandasTypes.float,
    SDCDFPandasTypes.double,
    SDCDFPandasTypes.int,
    SDCDFPandasTypes.long,
]


class Dataframe(object):

    def __init__(self, sdcdftype_, schema_json_):
        self.df = None
        self.schema_json = None
        self.df_schema = []
        self.type = None
        self.add_columns = {}
        self.nullable_columns = {}
        self.datetime_columns = {}
        self.default_column_values = {}
        self.json_columns = {}
        self.rename_columns = []
        self.drop_columns = []
        self.insert_query_ddl = None
        self.shape = (0,0)
        self.type = sdcdftype_
        if type(schema_json_) == str:
            self.schema_json = json.loads(schema_json_)
        elif type(schema_json_) == dict:
            self.schema_json = schema_json_
        else:
            raise Exception("Bad schema passed into dataframe.")

        self.database_table_name = None
        self.database_schema_name = None

        self.generate_schema()
        self.__history = []

    @staticmethod
    def generate_default_data_for_column(type_, row_count_):
        """
        Generates a pandas Series of values with certain types. Can then be
        used to update values in a dataframe.

        :param type_: Desired value type. If "datetime", will generate datetime
            values with utcnow. For other types, name will correlate the the value
            in enum SDCDFPandasTypes (if supported).
        :param row_count_: Int. Size to make series.
        :return: Pandas Series.
        """

        if type_ in SDCDFPandasTypes.__members__:
            if type_ == "datetime":
                return pd.Series([pd.Timestamp.utcnow().tz_localize(None).to_datetime64()
                        for i in range(row_count_)])
            elif type_ in ["null"]:
                return pd.Series([SDCDFPandasTypes[type_].value for i in range(row_count_)])
            else:
                raise Exception(f"Default value for type {type_} not supported.")
        else:
            raise Exception(f"No defaults available for {type_}")

    def generate_merge_query(self, table_name_, merge_table_name_):
        """
        Generates merge query code.
        :param table_name_: Snowflake table name to merge into.
        :param merge_table_name_: Snowflake table name to merge from.
        :return: String of merge query code.
        """

        def find_merge_keys(fields_):
            merge_keys = []
            for key in fields_:
                if "sf_merge_key" in key and key["sf_merge_key"] is True:
                    merge_keys.append(key["name"])

            if len(merge_keys) == 0:
                raise Exception("No Merge Keys were found in schema. Merge keys are required for upsert.")
            else:
                return merge_keys

        merge_keys = find_merge_keys(self.schema_json["fields"])

        columns = []
        for field in self.schema_json["fields"]:
            # Remove columns from merge query that we are dropping from dataframe
            if "drop_column" in field['type'] and field['type']["drop_column"]:
                logging.info(f'Column {field["name"]} removed from merge query.')

            else:
                columns.append(field["name"])

        query = SnowflakeDatabase.generate_merge_query(
            self.database_name, self.database_schema_name, table_name_,
            merge_table_name_, columns, merge_keys)

        return query

    def convert_columns_to_json(self):
        """
        Converts columns in a JSON-like format into useable JSON. Will replace
        single-quoted keys/values with double-quotes and ensure boolean
        values are in proper format for Snowflake JSON parsing.

        :return: None
        """

        for column in self.json_columns:
            self.df[column] = self.df[column].apply(
                lambda x: x.replace('\\n', '\\\\n').replace('\\r', '\\\\r')
                if isinstance(x, str) else x)
            self.df[column] = self.df[column].str.replace(
                r':\s*(false|FALSE)', ': False', regex=True)
            self.df[column] = self.df[column].str.replace(
                r':\s*(true|TRUE)', ': True', regex=True)
            self.df[column] = self.df[column].apply(
                lambda x: ast.literal_eval(x) if pd.notnull(x) else x).apply(
                lambda x: json.dumps(x) if pd.notnull(x) else x)

    def generate_insert_query_ddl(self):
        """
        Generates the DDL portion of an insert query based off of the dataframe
        data. Wraps JSON columns into appropriate JSON formatting functions.
        Example result:

            (START_DATE,
            EMPLOYEE_NUMBER,
            EMPLOYEE_DATA)

            select
            column1 as "START_DATE",
            column2 as "EMPLOYEE_NUMBER",
            PARSE_JSON(column3) as "EMPLOYEE_DATA"

            from values

        :return: DLL portion of query string.
        """

        # The part of the query that lists all the columns to be inserted in order.
        query_column_part = ', '.join(f'"{x}"' for x in self.df.columns)

        # The part of the query that handles the transformations of the columns
        # (if any), including adding PARSE_JSON() if JSON.
        query_select_part = ""
        count = 1
        for col in self.df.columns:
            if col in self.json_columns:
                q = f'PARSE_JSON(Column{count}) as "{col}", '
            else:
                q = f'Column{count} as "{col}", '
            query_select_part += q
            count += 1

        query = f'({query_column_part}) select {query_select_part[:-2]} from values '

        return query

    def generate_insert_query_values(self, index_start_, index_end_):
        """
        Generates the values portion of an insert query based off of the Dataframe
        data. Ensures values that should not be quoted as strings (numbers, boolean,
        etc.) aren't, and that None/Empty values in Dataframe are converted to NULL
        for inserting into table.
        Example result:

           ('2019-09-10', 5, {'EmployeeName': 'D. Engineer', 'EmployeeCode': 10}),
           ('2019-10-10', 6, {'EmployeeName': 'D. Analyst', 'EmployeeCode': 12}),
           ('2019-09-25', 9, {'EmployeeName': 'Mr. E', 'EmployeeCode': 15})

        :param index_start_: Starting index number of dataframe
        :param index_end_: Ending index number of dataframe
        :return: DLL portion of query string.
        """

        # Generate list of columns where the value should not be in quotes in
        # the insert statement. This include numbers and boolean values.
        non_quotable_columns = []
        for index, value in self.df.dtypes.iteritems():
            if type(value) in SDCDFNonQuotableTypes:
                non_quotable_columns.append(index)

        # The part of the query that lists the actual values to be inserted.
        # Converts any python None types into NULL for SQL consumption.
        query_value_part = ""
        for index, row in self.df[index_start_:index_end_].iterrows():
            count = 0
            line = "("
            for rec in row:
                if count < len(self.df.columns):
                    # Convert null values to string 'NULL' for query
                    if pd.isnull(rec):
                        line += f"NULL, "
                    else:
                        # Escape out single quotes in SQL value string by
                        # doubling single quote
                        if isinstance(rec, str) and "'" in rec:
                            rec = rec.replace("'", "''")
                        if self.df.columns[count] in non_quotable_columns:
                            line += f"{rec}, "
                        else:
                            line += f"'{rec}', "
                    count += 1
            query_value_part += line[:-2] + "), "

        return query_value_part

    def generate_complete_insert_query(self, index_start_, index_end_,
                                       table_uuid_=None):
        """
        Given the ddl and values part of the insert query, constructs the entire
        insert query needed to write to the database.

            insert into "COOLDATA"."AWESOME_TABLE"

            (START_DATE,
            END_DATE,
            EMPLOYEE_DATA)

            select
            column1 as "START_DATE",
            column2 as "END_DATE",
            PARSE_JSON(column3) as "EMPLOYEE_DATA"

            from values

            ('2019-09-10', 5, {'EmployeeName': 'D. Engineer', 'EmployeeCode': 10}),
            ('2019-10-10', 6, {'EmployeeName': 'D. Analyst', 'EmployeeCode': 12}),
            ('2019-09-25', 9, {'EmployeeName': 'Mr. E', 'EmployeeCode': 15})

        :param index_start_: Starting index number of dataframe
        :param index_end_: Ending index number of dataframe
        :param table_uuid_: String. UUID name to use in generation of temp
            table (for merging)
        :return: Complete insert query as string.
        """

        if not self.insert_query_ddl:
            query_ddl_part = self.generate_insert_query_ddl()

        query_value_part = self.generate_insert_query_values(
            index_start_, index_end_)

        if table_uuid_:
            table_name = self.database_table_name + f"_TEMP_{table_uuid_}"
        else:
            table_name = self.database_table_name

        complete_query = f'insert into "{self.database_schema_name}".' \
            f'"{table_name}" {query_ddl_part} {query_value_part[:-2]};'

        return complete_query

    def get_dataframe_schema(self, type_):
        '''
        @param type_: Schema type being requested (e.g. json, avro)
        '''

        pass

    def generate_schema(self):
        """
        This function will generate the proper schema for the dataframe.
        :return: None
        """
        column_type = None
        if "fields" not in self.schema_json:
            raise Exception("Malformed schema")
        if self.type == SDCDFTypes.PANDAS:
            for column in self.schema_json["fields"]:
                if "name" not in column or "type" not in column or "type" not \
                        in \
                        column["type"]:
                    raise Exception("Malformed schema")
                elif "logical_type" in column["type"]:
                    if column["type"]["logical_type"] not in \
                            SDCDFPandasTypes.__members__:
                        raise Exception(
                            f"Unknown logical_type "
                            f"{column['type']['logical_type']}")
                    else:
                        column_type = {
                            f"{column['name'].upper()}": SDCDFPandasTypes[
                                column["type"]["logical_type"]].value}
                        self.df_schema.append(column_type)
                else:
                    column_type = {
                        f"{column['name'].upper()}": SDCDFPandasTypes[
                            column["type"]["type"]].value}
                    self.df_schema.append(column_type)

                if "drop_column" in column and column["drop_column"]:
                    self.drop_columns.append(column["name"])
                    continue

                if "add_column" in column['type'] and column['type']["add_column"]:
                    key = list(column_type.keys())[0]
                    self.add_columns[key] = column_type[key]

                if "is_nullable" in column and column["is_nullable"]:
                    key = list(column_type.keys())[0]
                    self.nullable_columns[key] = column_type[key]

                if "logical_type" in column["type"] \
                        and column["type"]["logical_type"] == "datetime":
                    key = list(column_type.keys())[0]
                    self.datetime_columns[key] = column_type[key]

                if "logical_type" in column["type"] \
                        and column["type"]["logical_type"] == 'json':
                    key = list(column_type.keys())[0]
                    self.json_columns[key] = column_type[key]
                if "rename" in column:
                    self.rename_columns.append({column["name"].upper(): column["rename"].upper()})



                if "default_value" in column and column['default_value']:
                    key = list(column_type.keys())[0]
                    value = column['default_value']
                    self.default_column_values[key] = value

        elif self.type == SDCDFTypes.SPARK:
            return None

        else:
            raise Exception("Unknown dataframe type")

    def process_df(self, df_):
        """
        Processes a dataframe.
        :param df_: Dataframe to process.
        :return: None.
        """
        if self.type == SDCDFTypes.PANDAS:

            def handle_datetime(dt_):
                if dt_ is None or (isinstance(dt_, str) and len(dt_) == 0):
                    return pd.NaT
                p = pd.to_datetime(dt_, errors='coerce')

                if p == pd.NaT:
                    return p
                else:
                    return p.to_datetime64()

            self.df = df_
            self.df.columns = [str(x).upper().rstrip().lstrip() for x in df_.columns]

            # Replace and None or na types to NAN
            self.df = self.df.fillna(value=pd.np.nan)
            # Replace empty strings with NAN
            self.df = self.df.replace(r'^\s*$', np.nan, regex=True)

            column_names = []
            for datatype in self.df_schema:
                current_col = datatype

                key = list(datatype.keys())[0]
                column_names.append(key)
                if key not in self.df.columns and key not in \
                        self.add_columns and key not in self.nullable_columns:
                    raise Exception(
                        f"Column: {datatype} not in dataframe: "
                        f"{self.df.columns}")
                elif key in self.add_columns and key not in self.df.columns:
                    self.df[key] = Dataframe.generate_default_data_for_column(
                        "datetime", self.df.shape[0])
                elif key in self.nullable_columns and key not in self.df.columns:
                    self.df[key] = Dataframe.generate_default_data_for_column(
                        "null", self.df.shape[0])
                    if datatype[key] == SDCDFPandasTypes.datetime.value:
                        self.df[key] = self.df[key].map(handle_datetime)
                    else:
                        self.df[key] = self.df[key].astype(datatype)
                elif datatype[key] == SDCDFPandasTypes.datetime.value:
                    self.df[key] = self.df[key].map(handle_datetime)
                elif datatype[key] == SDCDFPandasTypes.string.value:
                    self.df[key] = self.df[key].map(
                        lambda x: str(x) if not isinstance(x, type(pd.np.nan)) else x)
                    self.df[key] = self.df[key].astype(datatype)
                elif datatype[key] == SDCDFPandasTypes.int.value \
                        or datatype[key] == SDCDFPandasTypes.long.value:
                    self.df[key] = pd.to_numeric(self.df[key])
                    self.df[key] = self.df[key].astype(datatype)

                else:
                    self.df[key] = self.df[key].astype(datatype)

            for column in self.df.columns:
                if column not in column_names:
                    logging.error(f"Dropping Unknown Column: {str(column)}")
                    self.df = self.df.drop(column, axis=1)
                if column in self.drop_columns:
                    logging.error(f"Dropping Column: {str(column)}")
                    self.df = self.df.drop(column, axis=1)

            for column_rename in self.rename_columns:
                self.df.rename(columns=column_rename, inplace=True)

            # reset shape after all the processing.
            self.shape = self.df.shape

        elif self.type == SDCDFTypes.SPARK:
            return None

        else:
            raise Exception("Unknown dataframe type")

        for column, default_value in self.default_column_values.items():
            self.df[column] = self.df[column].replace(np.NaN, default_value)

    def load_data(self, data_list_):
        """
        This function take in a list of flattened dictionaries.
        Nested structures not supported.
        :param data_list_:
        :return:
        """

        if self.type == SDCDFTypes.PANDAS:
            current_col = None

            try:
                self.df = pd.DataFrame(data_list_)
                self.process_df(self.df)
                self.shape = self.df.shape

            except Exception as e:
                logging.error(e)
                logging.error(f"failed on column {current_col}")
               # logging.error(f"Data: {data_list_}")
                raise Exception(
                    f"Unable to read data with schema {self.df_schema}")

        elif self.type == SDCDFTypes.SPARK:
            raise Exception("Spark dataframes unsupported currently.")

    def read_from_parquet(self, file_obj_):
        """
        This function will take a parquet file like object and load it into a dataframe.
        Useful for when you are reading parquet files into a SDCDF.
        :param file_obj_: File object to read from.
        :return: None
        """

        if self.type == SDCDFTypes.PANDAS:
            pd_df = pd.read_parquet(file_obj_, engine='auto')
            self.process_df(pd_df)

        elif self.type == SDCDFTypes.SPARK:
            raise Exception("Spark not currently supported")
        else:
            raise Exception("Invalid dataframe type to read parquet.")

    def sdcdf_to_sql(self, sdc_database_handle_, chunksize_=15000,
                     table_uuid_=None):
        """
        Iterates over dataframe, chunks and writes data to database.

        :param sdc_database_handle_: Database handle.
        :param chunksize_: Number of records to insert into database at one
            time. Default = 15,000. Note: Chunksize will be reset to the
            database-specific limit if it's more than that value. Limits:
                Snowflake = 15,000
        :param table_uuid_: UUID for TEMP table generation (if upserting).
            Default = None
        :return: None
        """

        if self.json_columns:
            self.convert_columns_to_json()

        if self.type == SDCDFTypes.PANDAS:
            if sdc_database_handle_.type == SDCDBTypes.SNOWFLAKE:

                total_records = len(self.df)
                if total_records > 0:
                    total_index = total_records - 1
                    index_start = 0
                    chunksize = chunksize_ if chunksize_ < 15000 else 15000

                    while (index_start < total_index) or index_start == 0:
                        index_end = index_start + chunksize
                        logging.info(f"Attempting to insert Dataframe records "
                                     f"into Snowflake. {index_start + 1:,} to "
                                     f"{min(index_end, total_records):,} "
                                     f"(out of {total_records:,})")
                        insert_query = self.generate_complete_insert_query(
                            index_start, index_end, table_uuid_)
                        sdc_database_handle_.execute_query(
                            insert_query, return_results_=True)
                        index_start += chunksize

    def write_dataframe_to_database(self, sdc_database_handle_, table_name_,
                                    schema_name_, upsert_=False, dedupe_=False):
        '''
        This function will write the dataframe to a database connection.
        Note that this function is append/create only so the connection
        need to have the proper permissions.
        @param sdc_database_handle_: Database connection/cursor object
        @param table_name_: Name of the database table you want to write to.
        @param schema_name_: Name of the schema in the database we are writing to.
        @param upsert_: Boolean: perform an upsert if this flag is set to true.
        '''

        self.database_table_name = table_name_
        self.database_schema_name = schema_name_
        self.database_name = sdc_database_handle_.database

        r, c = self.df.shape
        logging.info(f"Shape of dataframe: {self.df.shape}")
        if r == 0:
            raise Exception("No Data to write.")

        if self.type == SDCDFTypes.PANDAS:
            if sdc_database_handle_.type == SDCDBTypes.SNOWFLAKE:
                if not sdc_database_handle_.sqlalchemy:
                    raise Exception(
                        "Snowflake handle without SQLALCHEMY=true not supported.")

                if upsert_:
                    logging.info("Attempting upsert.")
                    table_uuid = str(uuid.uuid4()).replace("-", "_")
                    temp_table_name = f"{self.database_table_name}_TEMP_{table_uuid}"

                    # Create a empty temp table with same schema as main table
                    create_temp_table_clause = (
                        f"CREATE TABLE \"{self.database_schema_name}\".\"{temp_table_name}\" "
                        f"LIKE \"{self.database_schema_name}\".\"{self.database_table_name}\";"
                    )
                    sdc_database_handle_.execute_query(create_temp_table_clause)

                    # Load data into temp table
                    if self.json_columns:
                        self.sdcdf_to_sql(
                            sdc_database_handle_=sdc_database_handle_,
                            table_uuid_=table_uuid)

                    else:
                        self.df.to_sql(f"{temp_table_name}",
                                       sdc_database_handle_.connection,
                                       schema=self.database_schema_name,
                                       if_exists='append',
                                       index=False, chunksize=15000)

                    # If deduping, create deduped temp table, and generate merge query
                    if dedupe_:
                        deduped_table_name = f"{temp_table_name}_DEDUPED"

                        sdc_database_handle_.create_deduped_table(
                            self.database_name,
                            self.database_schema_name,
                            temp_table_name,
                            deduped_table_name
                        )
                        merge_query = self.generate_merge_query(
                            self.database_table_name,
                            deduped_table_name)
                    else:
                        merge_query = self.generate_merge_query(
                            self.database_table_name,
                            temp_table_name)

                    # Merge results from temp table into main table
                    logging.info("Merging TEMP table into main table.")
                    logging.info(merge_query)
                    sdc_database_handle_.execute_query(merge_query,
                                                       return_results_=True)

                    # Generate results for records insert/upserted
                    for row in sdc_database_handle_.get_results():
                        db_inserted = row[0]
                        db_upserted = row[1]

                    # Drop all temp tables
                    drop_temp_table_clause = f"DROP TABLE IF EXISTS \"{self.database_schema_name}\".\"{temp_table_name}\""
                    sdc_database_handle_.execute_query(drop_temp_table_clause, return_results_=True)
                    for result in sdc_database_handle_.get_results():
                        logging.info(result)

                    if dedupe_:
                        drop_deduped_table_clause = f"DROP TABLE IF EXISTS \"{self.database_schema_name}\".\"{deduped_table_name}\""
                        sdc_database_handle_.execute_query(
                            drop_deduped_table_clause, return_results_=True)
                        for result in sdc_database_handle_.get_results():
                            logging.info(result)


                else:

                    if self.json_columns:
                        self.sdcdf_to_sql(sdc_database_handle_)

                    else:
                        self.df.to_sql(table_name_, sdc_database_handle_.connection,
                                       schema=schema_name_, if_exists='append',
                                       index=False, chunksize=14000)

                    db_inserted = len(self.df)
                    db_upserted = 0

                return f"{db_inserted:,} row(s) inserted. " \
                    f"{db_upserted:,} row(s) upserted."

            elif sdc_database_handle_.type == SDCDBTypes.POSTGRES:
                raise Exception("Writing to Postgres not currently supported.")
            elif sdc_database_handle_.type == SDCDBTypes.MYSQL:
                raise Exception("Writing to MYSQL not currently supported.")
            elif sdc_database_handle_.type == SDCDBTypes.NEXUS:
                raise Exception("Writing to Nexus not currently supported.")
            else:
                raise Exception(f"Writing to {sdc_database_handle_.type.value} "
                                f"not currently supported.")

        elif self.type == SDCDFTypes.SPARK:
            raise Exception("Spark not currently supported")
        else:
            raise Exception("Invalid dataframe type to write.")

    def write_to_parquet(self, **kwargs):
        """
        This function will take a dataframe and return a file like object.
        to get all the raw data from the file object, use : file_obj.getvalue()
        :return: File like Object:
        """

        if self.type == SDCDFTypes.PANDAS:
            buffer = io.BytesIO()
            self.df.to_parquet(buffer, engine='auto', compression='snappy')
            buffer.seek(0)
            return buffer

        elif self.type == SDCDFTypes.SPARK:
            raise Exception("Spark not currently supported")
        else:
            raise Exception("Invalid dataframe type to write.")

    def write_to_csv(self, delimiter_=",", headers_=True, fieldnames_=[],
                     **kwargs):
        """
        This function will take a dataframe and return a file like object.
        to get all the raw data from the file object, use : file_obj.getvalue()
        :param delimiter_: File delimiter.
        :param headers_: Boolean. If true, file has headers. If false, file
            does not have headers.
        :param fieldnames_: List of field names (headers) of file. If not given,
            the first line of a file we be used as the headers.
        :return: File-like Object.
        """

        if self.type == SDCDFTypes.PANDAS:
            buffer = io.StringIO()
            self.df.to_csv(
                path_or_buf=buffer,
                index=False,
                sep=delimiter_,
                header=headers_,
                columns=fieldnames_
            )

            buffer.seek(0)
            buffer = io.BytesIO(buffer.read().encode())
            buffer.seek(0)

            return buffer

        elif self.type == SDCDFTypes.SPARK:
            raise Exception("Spark not currently supported")
        else:
            raise Exception("Invalid dataframe type to write.")

    def get_as_file_obj_from_endpoint_schema(self, endpoint_schema_):
        """
        This function will take a dataframe and return a file-like object.
        Note: Endpoint schema must be form the same schema that was used to
        construct the SDCDF

        :param endpoint_schema_: JSON data schema of endpoint.
        :return: File like Object
        """
        file_type = endpoint_schema_["file_info"]["type"]

        if file_type == "csv":
            args = SDCDataSchema.generate_file_output_args(self.schema_json, endpoint_schema_)
            return self.write_to_csv(**args)
        elif file_type == "parquet":
            args = SDCDataSchema.generate_file_output_args(self.schema_json, endpoint_schema_)
            return self.write_to_parquet(**args) if args else self.write_to_parquet()
        else:
            raise Exception("Invalid dataframe output type.")

    def scrub_pii(self):
        '''

        '''
        pass

    def perform_transformations(self, transform_list_, column_name_):
        '''
        @param transform_list_: List of transforamation function to perform
        @param column_name_: Column for transformation function to be
        performed on
        '''
        pass

    def drop_columns(self, column_list_):
        """
        Drops existing columns from dataframe inplace.

        :param column_list_: List of column names to drop.
        :return: Nothing
        """

        if column_list_ and isinstance(column_list_, list):
            column_list = list(set(column_list_))
            for column in column_list:
                if column not in self.df.columns.tolist():
                    logging.warning(f"Column '{column}' already does not exist "
                                    f"in DataFrame. Skipping...")
                    column_list.remove(column)
            logging.info(f"Dropping columns {column_list}")
            self.df.drop(column_list, axis=1, inplace=True)
            logging.info("Columns dropped!")

        else:
            raise Exception("Must past a list of columns to drop.")

    def cleanse_column_names(self, style_):
        """
        Renames column in dataframe via a preferred styling. Useful when needing
        to write to a sink that is sensitive to special characters or other
        naming conventions and we have no control over the source columnn
        naming.

        :param style_:  Style for column renaming. Current options:

            - "snowflake_friendly": Allows letters, numbers and underscores
                only. Special characters become "_". "#" changed to "NUM" to
                retain meaning. Example:

                "Default #: Addresses : Country (Full Name)"
                    becomes...
                "DEFAULT_NUM_ADDRESSES__COUNTRY_FULL_NAME"

        :return: None
        """

        if style_ == "snowflake" or style_ == "postgres":
            if self.type == SDCDFTypes.PANDAS:

                cols = self.df.columns.to_list()
                new_cols = [SnowflakeDatabase.clean_column_name(x) for x in cols]
                self.df.columns = new_cols

            elif self.type == SDCDFTypes.SPARK:
                raise Exception("Spark dataframes unsupported currently.")

        else:
            raise Exception(f"'{style_}' not an option.")

    def fill_in_column(self, column_name_, column_value_,
                       create_column_=False):
        """
        Updates a dataframe column with a new value (column-wide).
        :param column_name_: Name of dataframe column to be updated.
        :param column_value_: Value to update dataframe column with.
        :param create_column_: Boolean. If True, and if column to update does
            not exists in dataframe, create the column and fill in values.
        :return: None
        """

        column_name = column_name_.upper()
        col_exists = column_name in self.df.columns
        if not create_column_ and not col_exists:
            raise Exception(f"Cannot load values to column {column_name} as "
                            f"it does not exists in df. Cannot create unless "
                            f"explicitly allowed.")
        else:
            logging.info(f"Setting column {column_name} with value '{column_value_}'")
            self.df[column_name] = column_value_

    def compare_data_schema(self):
        '''

        '''
        pass

    def get_schema_from_registry(self):
        '''

        '''
        pass
