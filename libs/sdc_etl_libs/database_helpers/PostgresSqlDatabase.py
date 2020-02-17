#!/usr/bin/env python3

"""
##############################################################################
## PostgresSQL Database class
## Connects and queries a Postgres
##
# -------
#  For macs .. this is the install steps for psycopg2
#  brew install libpq
#  brew install openssl
#  export LIBRARY_PATH=$LIBRARY_PATH:/usr/local/opt/openssl/lib/
#  pip3 install psycopg2-------------------------------------------------------------------
#
# -----------------------------------------------------------------------------------
##############################################################################
"""

import psycopg2
import logging
import os
import sys
from sqlalchemy import create_engine

try:
    from airflow import AirflowException
except:
    pass

from sdc_etl_libs.database_helpers.Database import Database, SDCDBTypes


class PostgresSqlDatabase(Database):
    # Set logger up
    logging.basicConfig(
        format='%(levelname)s: %(asctime)s:  '
               '%(funcName)s: %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    def __init__(self, **kwargs):
        self.connection = None
        self.cursor = None
        self.airflow = None
        self.engine = None
        self.query_results = None
        self.type = SDCDBTypes.POSTGRES
        logging.info(kwargs)
        self.sqlalchemy = kwargs.get("sqlalchemy_", False)
        logging.info(self.sqlalchemy)
        logging.info("POSTGRES constructor.")

    def connect(self, server_, database_, username_, password_, port_=5432,
                airflow_=False):
        """
        Connects to POSTGRESSQL database.

        :param server_: server or IP
        :param database_: database
        :param user_: user
        :param pwd_: password
        :param port_: the port to connect too
        :param airflow_: Boolean.
            If True, triggers additional Airflow-specific logic
        :return: Nothing
        """

        self.airflow = airflow_
        logging.info("%s", "Connecting to PostgresSQL")

        if self.sqlalchemy:
            connection_string = (f"{username_}:{password_}@{server_}:{port_}/"
                                 f"{database_}")
            connection_string = f"postgresql+psycopg2://{connection_string}"
            self.engine = create_engine(connection_string)
            logging.info("Connecting to sqlalchemy PostgresSQL.")
            self.connection = self.engine.connect()
        else:
            self.connection = psycopg2.connect(user=username_,
                                               password =password_,
                                               host =server_,
                                               port =port_,
                                               database =database_)

        try:
            if self.sqlalchemy:
                self.cursor = self.connection
            else:
                self.cursor = self.connection.cursor()
            logging.info("Connected to PostgresSQL.")

        except Exception as e:
            if self.airflow:
                # For Airflow, forces task to fail and sets it up for re-try
                raise AirflowException("Error connecting to PostgresSQL. {}"
                                       .format(str(e)))
            else:
                logging.exception("Error connecting to PostgresSQL.")
                raise Exception

    def execute_query(self, query_, return_results_=False):
        """
        Executes query against POSTGRESSQL database.

        :param query_: Query string to run in Postgres.
        :param return_results_: Boolean. Defaults to False. If True,
            returns results of query to self.query_results
        :return: If return_results is True, returns the results of a query
            to self.query_results as a tuple or tuple-like proxy object,
            which supports index access.
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
                raise AirflowException("Error connecting to POSTGRESSQL. {}"
                                       .format(str(e)))
            else:
                logging.exception("Error running query.")
                raise e

    def get_results(self, fetch_method_='all', fetch_amount_=None):
        """
        Returns results from POSTGRESSQL cursor object at self.query_results.

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
            raise Exception("Cannot get results. Missing cusor/proxy object "
                            "with query. Use execute_query() where "
                            "return_results_=True.")

        if fetch_method_.lower() == 'all':
            results = self.query_results.fetchall()
            for result in results:
                yield result
            self.query_results = None

        elif fetch_method_.lower() == 'one':
            yield self.query_results.fetchone()
            self.query_results = None

        elif fetch_method_.lower() == 'many':
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
            raise Exception("{} if not a valid fetch method type."
                            .format(fetch_method_))

    def close_connection(self):
        """
        Closes database connection.
        """

        try:
            self.connection.close()
            logging.info("Connection closed.")

        except Exception:
            # We want to try and close connection, but, don't want to trigger
            # a fail state/re-try in Airflow if something goes wrong here.
            if self.airflow:
                pass
            else:
                logging.exception("Did not properly close connection.")
                raise Exception

    # Currently using pandas to write the data.
    def insert_data(self):
        """
        Inserts data into Postgres database.
        """

        pass
