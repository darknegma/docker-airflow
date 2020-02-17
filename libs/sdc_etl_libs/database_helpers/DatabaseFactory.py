
import os
import sys
import logging
sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from sdc_etl_libs.database_helpers.SnowflakeDatabase import SnowflakeDatabase
from sdc_etl_libs.database_helpers.NexusDatabase import NexusDatabase
from sdc_etl_libs.database_helpers.MySqlDatabase import MySqlDatabase
from sdc_etl_libs.database_helpers.PostgresSqlDatabase import PostgresSqlDatabase

logging.basicConfig(
    format='%(levelname)s: %(asctime)s:  '
           '%(funcName)s: %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class DatabaseFactory(object):

    @staticmethod
    def get_database(database_name_, **kwargs):
        logging.info(kwargs)
        if database_name_ == "snowflake":
            return SnowflakeDatabase(**kwargs)
        elif database_name_ == "nexus":
            return NexusDatabase()
        elif database_name_ == "mysql":
            return MySqlDatabase()
        elif database_name_ == "postgres":
            return PostgresSqlDatabase(**kwargs)
        else:
            raise Exception(f"{database_name_} is an invalid database option.")
