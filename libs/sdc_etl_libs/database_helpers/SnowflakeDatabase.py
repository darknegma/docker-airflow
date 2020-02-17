
import datetime
import logging
import re
import snowflake.connector
import sqlalchemy as sa
from snowflake.sqlalchemy import URL
from sdc_etl_libs.database_helpers.Database import Database, SDCDBTypes

try:
    from airflow import AirflowException
except:
    pass


class SnowflakeDatabase(Database):

    def __init__(self, **kwargs):
        """
        :param sqlalchemy_: Boolean. Defaults to False.
            If True, creates a db engine and establishes a Connection state
                with the sqlalchemy module. Returns a connection object which
                is used as the cursor.
            If False, establishes a connection with the snowflake.connector
                module and returns a cursor object.
        """
        self.type = SDCDBTypes.SNOWFLAKE
        self.engine = None
        self.connection = None
        self.cursor = None
        self.airflow = None
        self.sqlalchemy = kwargs.get("sqlalchemy_", False)
        self.query_results = None
        self.database = None
        self.table_name = None
        self.schema = None

        logging.info("Snowflake constructor.")

    def connect(self, warehouse_, database_, schema_, role_, user_,
                password_, account_, airflow_=False):
        """
        Makes connection to Snowflake and returns a cursor.

        :param warehouse_: Snowflake warehouse.
        :param database_: Snowflake database.
        :param schema_: Snowflake schema.
        :param role_: Snowflake role.
        :param user_: Snowflake user.
        :param password_: Snowflake password.
        :param account_: Snowflake account.
        :param airflow_: Boolean. If True, triggers additional
            Airflow-specific logic
        :return: Snowflake cursor object
        """

        self.airflow = airflow_
        self.database = database_
        self.schema = schema_
        # Role values should be double-quoted due to the use of periods
        if role_[0].strip() != '"' and role_[-1].strip() != '"':
            role = f'"{role_}"'
        else:
            role = role_

        logging.info("Connecting to Snowflake.")

        if self.sqlalchemy:

            self.engine = sa.create_engine(URL(
                user=user_,
                password=password_,
                account=account_,
                warehouse=warehouse_,
                database=database_,
                schema=schema_,
                role=role
            ))

            self.connection = self.engine.connect()

        else:

            self.connection = snowflake.connector.connect(
                user=user_,
                password=password_,
                account=account_,
                warehouse=warehouse_,
                database=database_,
                schema=schema_,
                role=role
            )

        try:
            if self.sqlalchemy:
                self.cursor = self.connection
            else:
                self.cursor = self.connection.cursor()

            logging.info("Connected to Snowflake.")

        except Exception as e:
            if self.airflow:
                # For Airflow, forces task to fail and sets it up for re-try
                raise AirflowException(f"Error connecting to Snowflake. {e}")
            else:
                logging.exception("Error connecting to Snowflake.")

    def execute_query(self, query_, return_results_=False):
        """
        Executes query against Snowflake database.

        :param query_: Query to run in Snowflake.
        :param return_results_: Boolean. Defaults to False. If True,
            returns a cursor/proxy object to self.query_results.
        :return: If return_results is True, a cursor/proxy object
            is returned.
        """

        try:
            if return_results_:
                logging.info("Fetching results...")
                self.query_results = self.cursor.execute(query_)
            else:
                self.cursor.execute(query_)
            logging.info("Query ran successfully.")

        except Exception as e:
            if self.airflow:
                # For Airflow, forces task to fail and set it up for re-try
                raise AirflowException(f"Error running query. {e}")
            else:
                logging.exception("Error running query.")
                raise Exception

    def get_results(self, fetch_method_='all', fetch_amount_=None):
        """
        Returns results from Snowflake cursor object at self.query_results.

        When running execute_query() with return_results_=True, a
        cursor/proxy object is returned at self.query_results_ which
        can be used to return records. When the records are exhausted,
        the cursor/proxy object will return 0.

        :param fetch_method_: Method by which to fetch results from
            the cursor/proxy object and place into memory.
            Can be one of the following options:
                'all' - Returns all records at once
                'one' - Returns a single record
                'many' - Returns a specified number of records at a time
        :param fetch_amount_: Number of rows to return at a time. If 'many'
            is selected for the fetch_method_, must provide a fetch_amount_
            as a positive integer. Does not work for 'all' or 'one'.
        :yields: Tuples or tuple-like objects for each record
            in the iteration.

        :returns: This function will return a generator object that can be
                  looped through to get results.

        # Example of simple iteration over rows
        while cnx.query_results:
            for row in cnx.get_results('many', 10):
                print(row)

        """

        if not self.query_results:
            raise Exception("Cannot get results. Missing cursor/proxy object "
                            "with query. Use execute_query() where "
                            "return_results_=True.")

        if fetch_method_ == 'all':
            results = self.query_results.fetchall()
            for result in results:
                yield result
            self.query_results = None

        elif fetch_method_ == 'one':
            yield self.query_results.fetchone()
            self.query_results = None

        elif fetch_method_ == 'many':
            if not isinstance(fetch_amount_, int) or fetch_amount_ < 0:
                raise Exception("Query results fetch type is set to 'many', "
                                "however, a positive integer was not passed "
                                "for fetch amount.")

            results = self.query_results.fetchmany(fetch_amount_)
            if len(results) > 0:
                for row in results:
                    yield row
            else:
                self.query_results = None

        else:
            raise Exception(f"{fetch_method_} if not a valid fetch method type.")

    def close_connection(self):
        """
        Closes Snowflake connection.
        If connection is being used in Airflow, will ignore any close
        exceptions to prevent an Airflow fail-state.
        """

        try:
            self.connection.close()
            self.engine.dispose() if self.sqlalchemy else None
            logging.info("Snowflake connection closed.")

        except Exception:
            # We want to try and close connection, but, don't want to trigger
            # a fail state/re-try in Airflow if something goes wrong here.
            if self.airflow:
                pass
            else:
                logging.exception("Did not properly close connection.")

    # TODO: Actually make this
    def insert_data(self):
        """
        Inserts data into Snowflake database.
        """

        pass

    def check_for_bookmark_table(self, database_, schema_, table_="ETL_FILES_LOADED"):
        """
        Checks to ensure the ETL file bookmark table actually exists and
        contains the necessary minimum columns for proper bookmarking. Useful
        when needing to know this table exists before starting the process of
        inserting into the base table (the table we want to load the actual
        records to).

        It is assumed that the bookmark table is located in the schema of the
        base  table(s) and contains the following three columns:
            - TABLE_NAME
            - FILE_NAME
            - DATE_LOADED

        :param database_: Database of base table
        :param schema_: Schema of base table
        :param table_: Name of bookmark table (defaults to standard
            'etl_files_loaded')
        :return: None
        """

        try:
            headers = self.connection.cursor().execute(f"select * from "
                                          f"{database_}.{schema_}.{table_} "
                                          f"where 1 = 0")
            if self.sqlalchemy:
                cols = headers.keys()
            else:
                cols = [col[0] for col in headers.description]

            diff = set(cols).difference({"TABLE_NAME", "FILE_NAME", "DATE_LOADED"})
            if diff:
                raise Exception(f"ETL bookmark table is missing columns: {diff}")

        except Exception as e:
            logging.exception("There was an issue verifying the bookmark table"
                              "is usable.")

    def get_file_bookmarks(self, database_, schema_, for_table_,
                           bookmark_table_='ETL_FILES_LOADED'):
        """
        Retrieves file names in the bookmark table.

        :param database_: Database of base table
        :param schema_: Schema of base table
        :param for_table_: Base table to retrieve file name records for
        :param bookmark_table_: Name of bookmark table (defaults to standard
            'etl_files_loaded')
        :return: List of files names already loaded to base table
        """

        self.execute_query(f'SELECT "FILE_NAME" FROM '
                           f'"{database_}"."{schema_}"."{bookmark_table_}" '
                           f'where TABLE_NAME = \'{for_table_}\' group by 1'.upper(),
                           return_results_=True)

        files = []
        for row in self.get_results():
            files.append(row[0])

        return files

    def update_file_bookmarks(self, database_, schema_, for_table_, file_name_,
                              bookmark_table_='ETL_FILES_LOADED'):
        """
        Updates a file bookmark table by inserting the base table name,
        file name loaded and date loaded.

        :param database_: Database of base table
        :param schema_: Schema of base table
        :param for_table_: Base table to record bookmark for
        :param file_name_: Name of file that was loaded to base table
        :param bookmark_table_: Name of bookmark table (defaults to standard
            'etl_files_loaded')
        :return: None
        """
        self.execute_query(f"insert into "
                           f"{database_}.{schema_}.{bookmark_table_} "
                           f"(TABLE_NAME, FILE_NAME, DATE_LOADED) "
                           f"values ('{for_table_.upper()}', '{file_name_}', "
                           f"'{datetime.datetime.now()}')")

    @staticmethod
    def clean_column_name(column_name_):
        """
        Cleans up a column name to be more Snowflake-friendly.
        :param column_name_: Column name to clean-up.
        :return: String of column name.
        """
        # Change # to NUM
        col = column_name_.replace('#', 'NUM')
        # Remove non-alphanumeric chars, trailing underscores & make uppercase
        col = re.sub("[^A-Za-z0-9]", "_", col).upper().rstrip("_")
        # Change any sequence of underscores to as single underscore
        col = re.sub("_{2,}","_", col)
        # If column starts with a number, put an underscore in front of it
        if re.match(r'[0-9]', col):
            col = "_" + col

        return col

    @staticmethod
    def generate_merge_query(database_, schema_, table_name_, merge_table_name_,
                             columns_, merge_keys_):
        """
        Generates merge query code.
        :param database_: Snowflake database.
        :param schema_: Snowflake schema.
        :param table_name_: Snowflake table name to merge into.
        :param merge_table_name_: Snowflake table name to merge from.
        :param columns_: List of columns to merge between tables.
        :param merge_keys_: List of column name to merge on.
        :return: String of merge query code.
        """

        query = (
            f"MERGE INTO \"{database_}\".\"{schema_}\".\"{table_name_}\" t1 \n"
            f"USING \"{database_}\".\"{schema_}\".\"{merge_table_name_}\" t2 ON "
        )

        update_clause = ""
        insert_clause = ""

        merge_columns = [SnowflakeDatabase.clean_column_name(key) for key in merge_keys_]

        on_clause = " and ".join(
            f't1."{column}" = t2."{column}"' for column in merge_columns)

        count = 0
        for column in columns_:

            clean_column_name = SnowflakeDatabase.clean_column_name(column)

            # Construct update and insert clauses.
            if len(columns_) > 1 and \
                    count != (len(columns_) - 1):
                update_clause += f'"{clean_column_name}"=t2."{clean_column_name}",\n'
                insert_clause += f'"{clean_column_name}",\n'

            else:
                update_clause += f'"{clean_column_name}"=t2."{clean_column_name}"'
                insert_clause += f'"{clean_column_name}"'

            count += 1

        query += on_clause + "\nWHEN MATCHED THEN UPDATE SET\n"
        query += update_clause + "\n"
        query += f"WHEN NOT MATCHED THEN INSERT(\n{insert_clause}\n)\n"
        query += f"VALUES(\n{insert_clause}\n);"

        return query

    def create_deduped_table(self, database_, schema_, table_name_, deduped_table_name_=None):
        """
        Creates a deduped table from an existing table in Snowflake.
        :param database_: Snowflake database.
        :param schema_: Snowflake schema.
        :param table_name_: Snowflake table to be deduped.
        :param deduped_table_name_: Name for deduped table. Default = table_name + 'DEDUPED'.
        :return: None.
        """

        if not deduped_table_name_:
            deduped_table_name = f"{table_name_}_DEDUPED"
        else:
            deduped_table_name = deduped_table_name_

        create_deduped_table_clause = (
            f"CREATE OR REPLACE TABLE \"{database_}\".\"{schema_}\".\"{deduped_table_name}\" AS "
            f"SELECT DISTINCT * FROM \"{database_}\".\"{schema_}\".\"{table_name_}\""
        )

        self.execute_query(create_deduped_table_clause)

