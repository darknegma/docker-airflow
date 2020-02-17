import os
from datetime import datetime, timedelta
from enum import Enum
import json
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
import logging

from sdc_etl_libs.aws_helpers.aws_helpers import AWSHelpers
from sdc_etl_libs.database_helpers.DatabaseFactory import DatabaseFactory
from sdc_etl_libs.airflow_helpers.AirflowHelpers import AirflowHelpers
from sdc_etl_libs.aws_helpers.S3Data import S3Data
from sdc_etl_libs.sdc_filetransfer.ProcessingLogger import ProcessingLogger, SDCPROCESSING_LOGGER_TYPES
#from sdc_etl_libs.sdc_data_exchange.SDCDataExchange import SDCDataExchange
from sdc_etl_libs.sdc_file_helpers.SDCFileHelpers import SDCFileHelpers
from sdc_etl_libs.sdc_dataframe.Dataframe import Dataframe, SDCDFTypes


# Set logger up
logging.basicConfig(
    format='%(levelname)s: %(asctime)s:  %(funcName)s: %(message)s')  # %(lineno)d ,level=logging.DEBUG
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if Variable.get("environment") == "development":
    db_creds = AWSHelpers.get_secrets(
        "snowflake/service_account/airflow")
    snowflake_user = db_creds["username"]
    snowflake_pwd = db_creds["password"]
    snowflake_account = db_creds["account"]
    snowflake_db = "DATAENGINEERING"
    snowflake_schema = "NEUSTAR"
    snowflake_warehouse = db_creds["warehouse"]
    snowflake_role = 'AIRFLOW_SERVICE_ROLE'
    # aws
    bucket_name = 'sdc-marketing-vendors'
    bucket_prefix = 'neustar/'
    file_prefix = 'SmileDirect_Fact_Funnel_Extract'
    file_suffix = '.gz'
    region = 'us-west-1'

elif Variable.get("environment") == "production":
    db_creds = AWSHelpers.get_secrets(
        "snowflake/service_account/airflow")
    snowflake_user = db_creds["username"]
    snowflake_pwd = db_creds["password"]
    snowflake_account = db_creds["account"]
    snowflake_db = "MARKETING"
    snowflake_schema = "NEUSTAR"
    snowflake_warehouse = db_creds["warehouse"]
    snowflake_role = 'AIRFLOW_SERVICE_ROLE'
    # aws
    bucket_name = 'sdc-marketing-vendors'
    bucket_prefix = 'neustar/'
    file_prefix = 'SmileDirect_Fact_Funnel_Extract'
    file_suffix = '.gz'
    region = 'us-west-1'

def process_files(**kwargs):
    process_failed_runs()
    process_new_files()


def get_files():

    # Prep snowflake connection
    snowflake = DatabaseFactory.get_database("snowflake", sqlalchemy_=True)
    snowflake.connect(warehouse_=snowflake_warehouse,
                      database_=snowflake_db,
                      schema_=snowflake_schema,
                      role_=snowflake_role,
                      user_=snowflake_user,
                      password_=snowflake_pwd,
                      account_=snowflake_account,
                      airflow_=True)

    # Prep custom logger
    file_logger = ProcessingLogger(snowflake, snowflake_schema, snowflake_db, '"etl_files_loaded"')

    files_to_process = []  # list of files to process - the s3 bucket - minus what we have prev processed
    files_processed_database = []  # list of files to process - the snowflake log table of previous processed
    s3_files = []       # list of files to process from the s3 bucket.. after removing full path
    s3_files_temp = []  # temp working list until i figure why the helper function is returning full path

    # get files in the s3 bucket
    s3_files_temp = S3Data.get_file_list_s3(
        bucket_name_=bucket_name,
        prefix_=bucket_prefix
    )

    # Remove no name files
    if '' in s3_files_temp:
        s3_files_temp.remove('')

    for itm in s3_files_temp:
        s3_files.append(itm.replace('s3://sdc-marketing-vendors/neustar/', ''))

    # see what we already have logged as processed
    files_processed_database = file_logger.get_files_processed()

    # create a list of ones we have not yet processed
    files_to_process = list(set(s3_files) - set(files_processed_database))

    return files_to_process


def process_failed_runs():

    file_id = 0
    icounter = 0

    try:
        # Prep snowflake connection
        snowflake = DatabaseFactory.get_database("snowflake", sqlalchemy_=True)
        snowflake.connect(warehouse_=snowflake_warehouse,
                          database_=snowflake_db,
                          schema_=snowflake_schema,
                          role_=snowflake_role,
                          user_=snowflake_user,
                          password_=snowflake_pwd,
                          account_=snowflake_account,
                          airflow_=True)

        # Prep custom logger
        file_logger = ProcessingLogger(snowflake, snowflake_schema, snowflake_db, '"etl_files_loaded"')

        # retrieve any files that were failed or stuck in processing
        filter_list = []
        filter_list.append(SDCPROCESSING_LOGGER_TYPES.FAILED.value)
        filter_list.append(SDCPROCESSING_LOGGER_TYPES.PROCESSING.value)
        file_recovery = file_logger.get_files_by_status(filter_list)

        data_schema_file = SDCFileHelpers.get_file_path("schema", 'Neustar/neustar_fact_funnel.json')
        df_schema = json.loads(open(data_schema_file).read())

        for rec_file in file_recovery:
            filename = rec_file["file_name"]
            number_of_rows = rec_file["number_of_rows"]
            file_id = rec_file["file_id"]
            logger.info("%s", f"Retrieving file from S3 in recovery mode: {filename}")

            large_file = S3Data(bucket_name_=bucket_name,
                                    prefix_=bucket_prefix,
                                    file_=filename,
                                    df_schema_=df_schema,
                                    check_headers_= True,
                                    #column_headers_text_to_check_=b'fact_funnel.unattributed_rev',
                                    file_type_='tsv',
                                    compression_type_='gz',
                                    region_=region,
                                    decode_='utf-8')

            large_file.load_data()
            table = large_file.table
            schema = snowflake_schema  #L.schema

            # loop variables
            filesize = large_file.get_file_record_count()
            icounter = number_of_rows + 1 # --> start where you left off
            chunk = 15000

            # begin looping
            while icounter <= filesize:
                data = large_file.get_records(icounter, chunk, file_id)
                actual_chunk_size = len(data)

                if actual_chunk_size > 0:
                    df = []
                    df = Dataframe(SDCDFTypes.PANDAS, df_schema)
                    df.load_data(data)
                    df.write_dataframe_to_database(snowflake, table, schema)
                    logger.info("%s", "Records inserted into snowflake table")
                    file_logger.update_status(file_id, icounter - 1 + actual_chunk_size, SDCPROCESSING_LOGGER_TYPES.PROCESSING)

                icounter += chunk

            file_logger.update_status(file_id, filesize, SDCPROCESSING_LOGGER_TYPES.FINISHED)

    except Exception as e:
        logger.error('%s', "get_files_processed, Error is :", exc_info=True)
        file_logger.update_status(file_id, icounter - 1, SDCPROCESSING_LOGGER_TYPES.FAILED)


def process_new_files():
    file_id = 0
    icounter = 0

    try:
        # Prep snowflake connection
        snowflake = DatabaseFactory.get_database("snowflake", sqlalchemy_=True)
        snowflake.connect(warehouse_=snowflake_warehouse,
                          database_=snowflake_db,
                          schema_=snowflake_schema,
                          role_=snowflake_role,
                          user_=snowflake_user,
                          password_=snowflake_pwd,
                          account_=snowflake_account,
                          airflow_=True)

        # Prep custom logger
        file_logger = ProcessingLogger(snowflake, snowflake_schema, snowflake_db, '"etl_files_loaded"')

        files_to_process = get_files()

        data_schema_file = SDCFileHelpers.get_file_path("schema", 'Neustar/neustar_fact_funnel.json')
        df_schema = json.loads(open(data_schema_file).read())

        for file in files_to_process:
            logger.info("%s", f"Retrieving files from S3 in normal mode: {file}")

            large_file = S3Data(bucket_name_=bucket_name,
                                prefix_=bucket_prefix,
                                file_=file,
                                df_schema_=df_schema,
                                check_headers_=True,
                                # column_headers_text_to_check_=b'fact_funnel.unattributed_rev',
                                file_type_='tsv',
                                compression_type_='gz',
                                region_=region,
                                decode_='utf-8')

            large_file.load_data()
            table = large_file.table
            schema = snowflake_schema  # L.schema

            # loop variables
            filesize = large_file.get_file_record_count()
            icounter = 1
            chunk = 15000

            # begin logging
            # set the file id here if you want to add a FK reference from the details records back to the log table
            file_id = file_logger.log_begin(file)

            # begin looping
            while icounter <= filesize:
                data = large_file.get_records(icounter, chunk, file_id)
                actual_chunk_size = len(data)

                if actual_chunk_size > 0:
                    df = []
                    df = Dataframe(SDCDFTypes.PANDAS, df_schema)
                    df.load_data(data)
                    df.write_dataframe_to_database(snowflake, table, schema)
                    logger.info("%s", "Records inserted into snowflake table")
                    file_logger.update_status(file_id, icounter - 1 + actual_chunk_size, SDCPROCESSING_LOGGER_TYPES.PROCESSING)

                icounter += chunk

            file_logger.update_status(file_id, filesize, SDCPROCESSING_LOGGER_TYPES.FINISHED)

    except Exception as e:
        logger.error('%s', "get_files_processed, Error is :", exc_info=True)
        file_logger.update_status(file_id, icounter - 1, SDCPROCESSING_LOGGER_TYPES.FAILED)


default_args = {
    'owner': 'glen.brazil',
    'start_date': datetime(2019, 10, 16),
    'email': 'AirflowHelpers.get_dag_emails("data-eng")',  # glen.brazil@smiledirectclub.com'
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'neustar_Fact_Funnel',
    default_args=default_args,
    schedule_interval='00 10 * * *',
    dagrun_timeout=timedelta(hours=2)
)

nuestar_fact_funnel_snowflake = PythonOperator(
    task_id='task_neustar_fact_funnel_snowflake',
    provide_context=True,
    python_callable=process_files,
    dag=dag)

email_status = EmailOperator(
    task_id='Neustar_FactFunnel_status_email',
    to='AirflowHelpers.get_dag_emails("data-eng")',  # glen.brazil@smiledirectclub.com'
    retries=3,
    dag=dag,
    subject="AIRFLOW - COMPLETED: Neustar Fact Funnel task Completed",
    # html_content="yeah"
    html_content="""<h3>Neustar  Import to snowflake has <font color=\"green\">completed.</font></h3>
      <br>\
      {{task_instance.xcom_pull(task_ids='task_neustar_fact_funnel_snowflake', key='etl_results') }}
      <br>\
      """
)

nuestar_fact_funnel_snowflake >> email_status
