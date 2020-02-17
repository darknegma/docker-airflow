#!/usr/bin/env python3

"""
##############################################################################
## gjb
## 06/10/19
## https://smiledirectclub.atlassian.net/browse/BI-1835
## MySQL Database class
## Connects and queries a mySQL database (Used by Proship) software
##
# --------------------------------------------------------------------------
# See here for the CA Nexus setup..
# https://smiledirectclub.atlassian.net/wiki/spaces/EN/pages/370868845/CA+OnyxCeph
# -----------------------------------------------------------------------------------
##############################################################################
"""

import pyodbc
import logging
import os
import sys

try:
    from airflow import AirflowException
except:
    pass

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from Database import Database, SDCDBTypes


class MySqlDatabase(Database):
    # Set logger up
    logging.basicConfig(
        format='%(levelname)s: %(asctime)s:  '
               '%(funcName)s: %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    def __init__(self):
        self.connection = None
        self.cursor = None
        self.airflow = None
        self.query_results = None
        self.type = SDCDBTypes.MYSQL

        logging.info("MySql constructor.")

    def connect(self, server_, database_, user_, pwd_,
                driver_='{MySQL ODBC 3.51 Driver}', airflow_=False):
        """
        Connects to MySQL database.

        :param server_: server or IP
        :param database_: database
        :param user_: user
        :param pwd_: password
        :param driver_: driver .. defaulted to version 3.5 if not passed
        :param airflow_: Boolean.
            If True, triggers additional Airflow-specific logic
        :return: Nothing
        """

        self.airflow = airflow_

        logging.info("%s", "Connecting to MySQL")

        self.connection = \
            pyodbc.connect("DRIVER={driver}; "
                           "SERVER={server}; "
                           "DATABASE={database};"
                           "UID={user}; "
                           "PASSWORD={pwd};"
                           "OPTION=3;"
                           .format(driver=driver_, server=server_,
                                   database=database_, user=user_,
                                   pwd=pwd_))

        try:
            self.cursor = self.connection.cursor()
            logging.info("Connected to MySQL.")

        except Exception as e:
            if self.airflow:
                # For Airflow, forces task to fail and sets it up for re-try
                raise AirflowException("Error connecting to mySQL. {}"
                                       .format(str(e)))
            else:
                logging.exception("Error connecting to MySQL.")
                raise Exception

    def execute_query(self, query_, return_results_=False):
        """
        Executes query against MySQL database.

        :param query_: Query string to run in MySQL.
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
                raise AirflowException("Error connecting to Snowflake. {}"
                                       .format(str(e)))
            else:
                logging.exception("Error running query.")
                raise e

    def get_results(self, fetch_method_='all', fetch_amount_=None):
        """
        Returns results from Nexus cursor object at self.query_results.

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
            # self.query_results = None

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

    # Will never insert into mysql.. i dont think
    def insert_data(self):
        """
        Inserts data into MySQL database.
        """

        pass
