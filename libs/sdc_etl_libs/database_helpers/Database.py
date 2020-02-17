
import logging
from enum import Enum


class SDCDBTypes(Enum):
    SNOWFLAKE = "snowflake"
    MYSQL = "mysql"
    NEXUS = "nexus"
    POSTGRES = "postgres"

class Database(object):

    connection = None

    def __init__(self):
        logging.info("Base constructor.")

    def connect(self, **kwargs):
        """
        Creates database connections and cursor object (if applicable).
        :param kwargs:
        :return: Cursor object (if applicable) to self.cursor
        """

        logging.error("You should not be using the base class "
                      "connect function.")

    def execute_query(self, query_):
        """
        Executes query and returns a cusir/proxy object to
            self.query_results if selected to do so.
        :param query_: A query string to execute against a database.
        """

        logging.error("You should not be using the base class "
                      "execute_query function.")

    def get_results(self):
        """
        Returns results from cursor/proxy object at self.query_results.
        :return: A tuple or tuple-like object of results
        """

        logging.error("You should not be using the base class "
                      "get_results function.")

    def close_connection(self):
        """
        Closes database connection.
        :return: None
        """

        logging.error("You should not be using the base class "
                      "close_connection function.")

    def insert_data(self, data_list_, upsert=False):
        """
        Inserts data into database.
        :param data_list_:
        :param upsert:
        """

        logging.error("You should not be using the base class "
                      "insert_data function.")
