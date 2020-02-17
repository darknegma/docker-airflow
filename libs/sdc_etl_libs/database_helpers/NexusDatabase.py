#!/usr/bin/env python3-32

"""
##############################################################################
## gjb
## 06/10/19
## https://smiledirectclub.atlassian.net/browse/BI-1835
## Nexus Database class
## Connects and queries a nexus database (Used by CA Digital software
##
# --------------------------------------------------------------------------
# See here for the CA Nexus setup..
# https://smiledirectclub.atlassian.net/wiki/spaces/EN/pages/377225552/Proship+-+Data+Engineering
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


class NexusDatabase(Database):

    def __init__(self):
        self.connection = None
        self.cursor = None
        self.airflow = None
        self.query_results = None
        self.type = SDCDBTypes.NEXUS

        # Set logger up
        logging.basicConfig(
            format='%(levelname)s: %(asctime)s:  %(funcName)s: %(message)s')
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        logging.info("Nexus constructor.")

    def connect(self, dsn_, driver_='{NexusDB V3.08 Driver}', airflow_=False):
        """
        Connect to Nexus database.
        :param dsn_: Name should be the name used in setting up the dsn.
        :param driver_: Driver.
        :param airflow_: Boolean. If True, triggers additional
            Airflow-specific logic
        :return: Nothing - Initiates a connection in the object
        """

        self.airflow = airflow_

        logging.info("%s", "Connecting to DSN")
        self.connection = \
            pyodbc.connect("""DSN={dsn}; DRIVER ={driver}"""
                           .format(dsn=dsn_, driver=driver_))

        try:
            self.cursor = self.connection.cursor()
            logging.info("Connected to DSN.")

        except Exception as e:
            if self.airflow:
                # For Airflow, forces task to fail and sets it up for re-try
                raise AirflowException("Error connecting to DSN. {}"
                                       .format(str(e)))

            else:
                logging.exception("Error connecting to DSN.")
                raise Exception

    def execute_query(self, query_, return_results_=False):
        """
        Executes query against Nexus database.
        :param query_: Query to run in Nexus.
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
                raise AirflowException("Error running query. {}"
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
            raise Exception("Cannot get results. Missing cusor/proxy "
                            "object with query. Use execute_query() where "
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

    def __del__(self):
        pass

    # Will never insert into Nexus.. i dont think
    def insert_data(self):
        """
        Inserts data into Nexus database.
        """

        pass