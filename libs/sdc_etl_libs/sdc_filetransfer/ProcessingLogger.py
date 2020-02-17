
import logging
import datetime
from enum import Enum
from sdc_etl_libs.database_helpers.SnowflakeDatabase import SnowflakeDatabase


# Set logger up
logging.basicConfig(format='%(levelname)s: %(asctime)s:  %(funcName)s: %(message)s')  # %(lineno)d ,level=logging.DEBUG
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class SDCPROCESSING_LOGGER_TYPES(Enum):
    BEGINNING = "BEGINNING"
    PROCESSING = "PROCESSING"
    FAILED = "FAILED"
    FINISHED = "FINISHED"

class ProcessingLogger(object):
    """
    """

    def __init__(self, sdc_database_handle_:SnowflakeDatabase, schema_ ='', database_ = '', table_name_='"etl_files_loaded"'):

        self.table_name_ = table_name_
        self.schema_ = schema_
        self.database  = database_

        if isinstance(sdc_database_handle_, SnowflakeDatabase):
            self.snowflake_ = sdc_database_handle_
        else:
            logger.error('%s', "Database must be of type snowflake", exc_info=False)
            raise Exception

    def __get_file_id(self):
        """
           Private Method
           Retrieves an id will use to identify the files
           :return: File id to use
           """

        sql_string = f"""SELECT coalesce(MAX(FILE_ID),0) AS MAXID
         FROM "{self.database}"."{self.schema_}".{self.table_name_} """

        try:
            self.snowflake_.execute_query(sql_string, True)
            rows = self.snowflake_.get_results("all")
            for row in rows:
                maxid = int(row[0]) + 1

        except Exception as e:
            maxid = "NA"  # force an error
            logger.error('%s', f"get_file_id, Error is : {str(e)}", exc_info=False)

        return maxid

    def log_begin(self, file_):
        """
         Writes a status record back to our log table to indicate processing has begun
             :param file_: file name of file we are processing
             :return: file id to add to detail recordd
         """

        datevalue = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")  # for logging

        file_id = self.__get_file_id()

        sql_string = f"""INSERT INTO "{self.database}"."{self.schema_}".{self.table_name_}
                           (FILE_ID, FILE_NAME, DATE_LOADED, NUMBER_OF_ROWS,LOAD_STATUS)
                            VALUES({file_id},'{file_}','{datevalue}',{0}, 'BEGINNING') 
                         """
        try:
            self.snowflake_.execute_query(sql_string, False)

        except Exception as e:
            logger.error('%s', f"log_begin, Error is : {str(e)}", exc_info=False)

        return file_id

    def update_status(self, file_id_, how_many_records_, status_:SDCPROCESSING_LOGGER_TYPES):
        """
          Updates a status record  in our log table
          :param file_id_: file identifier .. this is a foreign key in the detail table
          :param how_many_records_:  How many rows were in the file
          :param status_: Processing status

          :return: None
          """

        sql_string = f"""UPDATE "{self.database}"."{self.schema_}".{self.table_name_}
                           SET NUMBER_OF_ROWS ={how_many_records_},
                           LOAD_STATUS='{status_.value}' 
                           WHERE FILE_ID ={file_id_}
                        """

        try:
            self.snowflake_.execute_query(sql_string, False)
        except Exception as e:
            logger.error('%s', f"update_status, Error is : {str(e)}", exc_info=False)

    def get_files_by_status(self, status_:list):
        """
        :param status_: list of statuss to filter for
         Example usage:
            filter.append(SDCPROCESSING_LOGGER_TYPES.FAILED.value)
            filter.append(SDCPROCESSING_LOGGER_TYPES.PROCESSING.value)
            files = P.get_all_processed(filter)
        :return: LIST OF dictionary of files processed by status
        """

        # Our return stuff
        files = {}
        file_list = []

        try:
            if not isinstance(status_, list):
                raise Exception()
            else:
                filter_str = ''
                filter_str = ','.join("'" + status + "'" for status in status_)
                sql_string = f"""SELECT   FILE_NAME,
                                          NUMBER_OF_ROWS,
                                          FILE_ID,
                                          to_char(DATE_LOADED, 'YYYY-MM-DD HH:mi') as DATE_LOADED
                                  FROM "{self.database}"."{self.schema_}".{self.table_name_}
                                 """
                if not filter_str == '':
                    sql_string += f" WHERE LOAD_STATUS IN ({filter_str})"

            self.snowflake_.execute_query(sql_string, True)
            for rows in self.snowflake_.get_results("all"):
                files["file_name"] = rows[0]
                files["number_of_rows"] = rows[1]
                files["file_id"] = rows[2]
                files["date_loaded"] = rows[3]

                file_list.append(files.copy())
                files.clear()

        except Exception as e:
            logger.error('%s', f"get_files_processed, Error is : {str(e)}", exc_info=False)

        return file_list

    def get_files_processed(self):
        """
        gets a list of files we previously processed from our log table
        :return: list of files that began processing but failed in the middle of the processing
        """

        files = []

        try:
            finished_ = f"""SELECT FILE_NAME
                               FROM "{self.database}"."{self.schema_}".{self.table_name_}
                               WHERE LOAD_STATUS IN ('FINISHED')
                           """
            sql_string = finished_

            self.snowflake_.execute_query(sql_string, True)
            for rows in self.snowflake_.get_results("all"):
                files.append(str(rows).split("'")[1])

        except Exception as e:
            logger.error('%s', f"get_files_processed, Error is : {str(e)}", exc_info=False)

        return files
