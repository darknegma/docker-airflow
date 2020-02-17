
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from sdc_etl_libs.api_helpers.APIFactory import APIFactory
from sdc_etl_libs.database_helpers.DatabaseFactory import DatabaseFactory
from sdc_etl_libs.aws_helpers.aws_helpers import AWSHelpers
from sdc_etl_libs.airflow_helpers.AirflowHelpers import AirflowHelpers

if Variable.get("environment") == "development":

    db_creds = AWSHelpers.get_secrets(
        "snowflake/service_account/airflow")

    snowflake_user = db_creds["username"]
    snowflake_pwd = db_creds["password"]
    snowflake_account = db_creds["account"]
    snowflake_db = "DATAENGINEERING"
    snowflake_schema = "ULTIPRO"
    snowflake_warehouse = db_creds["warehouse"]
    snowflake_role = '"AIRFLOW_SERVICE_ROLE"'

elif Variable.get("environment") == "production":
    db_creds = AWSHelpers.get_secrets(
        "snowflake/service_account/airflow")

    snowflake_user = db_creds["username"]
    snowflake_pwd = db_creds["password"]
    snowflake_account = db_creds["account"]
    snowflake_db = "HRIS_DATA"
    snowflake_schema = "ULTIPRO"
    snowflake_warehouse = db_creds["warehouse"]
    snowflake_role = '"AIRFLOW_SERVICE_ROLE"'

# Some tasks, like most Time Management ones, re-pull all the data each time,
# so no need to set start date to beg. of Ultipro service.
START_DATE_NO_FILTER_TASKS = datetime(2019, 10, 1)

def get_restapi_employment_details(table_name_, **kwargs):
    ultipro = APIFactory.get_api("ultipro-restapis")
    snowflake_dbhandle = DatabaseFactory.get_database("snowflake", sqlalchemy_=True)
    snowflake_dbhandle.connect(kwargs["snowflake_warehouse"],
                               kwargs["snowflake_db"],
                               kwargs["snowflake_schema"],
                               kwargs["snowflake_role"],
                               kwargs["snowflake_user"],
                               kwargs["snowflake_pwd"],
                               kwargs["snowflake_account"], airflow_=True
                               )
    logging.info("Grabbing Employee details")
    df = ultipro.get_employment_details(
        filter_=ultipro.get_daily_filter(
            datetime_=kwargs["ds"], days_=3, type_=1, field_='dateTimeChanged'))
    logging.info("Attempting to write to snowflake")

    if not isinstance(df, type(None)):
        results = df.write_dataframe_to_database(
            snowflake_dbhandle, table_name_, 'ULTIPRO', upsert_=True)
        logging.info("Done writing to Snowflake!")
    else:
        results = f"Received no data for this run {kwargs['execution_date']}"
        logging.error(results)

    kwargs['ti'].xcom_push(key='etl_results', value="<b>{}</b>: {}"
                           .format(table_name_, results))


def get_restapi_employee_changes(table_name_, **kwargs):
    ultipro = APIFactory.get_api("ultipro-restapis")
    snowflake_dbhandle = DatabaseFactory.get_database("snowflake", sqlalchemy_=True)
    snowflake_dbhandle.connect(kwargs["snowflake_warehouse"],
                               kwargs["snowflake_db"],
                               kwargs["snowflake_schema"],
                               kwargs["snowflake_role"],
                               kwargs["snowflake_user"],
                               kwargs["snowflake_pwd"],
                               kwargs["snowflake_account"], airflow_=True)

    logging.info("Grabbing Employee changes")
    df = ultipro.get_employee_changes(
        filter_=ultipro.get_daily_filter(
            datetime_=kwargs["ds"], days_=1, type_=2))
    logging.info("Attempting to write to snowflake")

    if not isinstance(df, type(None)):
        results = df.write_dataframe_to_database(
            snowflake_dbhandle, table_name_, 'ULTIPRO')
        logging.info("Done writing to Snowflake!")
    else:
        results = f"Received no data for this run {kwargs['execution_date']}"
        logging.error(results)

    kwargs['ti'].xcom_push(key='etl_results', value="<b>{}</b>: {}"
                           .format(table_name_, results))

def get_restapi_compensation_details(table_name_, **kwargs):
    ultipro = APIFactory.get_api("ultipro-restapis")
    snowflake_dbhandle = DatabaseFactory.get_database("snowflake", sqlalchemy_=True)
    snowflake_dbhandle.connect(kwargs["snowflake_warehouse"],
                               kwargs["snowflake_db"],
                               kwargs["snowflake_schema"],
                               kwargs["snowflake_role"],
                               kwargs["snowflake_user"],
                               kwargs["snowflake_pwd"],
                               kwargs["snowflake_account"], airflow_=True)

    logging.info("Grabbing compensation data.")
    df = ultipro.get_compensation_details()
    logging.info("Attempting to write to snowflake")

    if not isinstance(df, type(None)):
        results = df.write_dataframe_to_database(
            snowflake_dbhandle, table_name_,'ULTIPRO', upsert_=True)
        logging.info("Done writing to Snowflake!")
    else:
        results = f"Received no data for this run {kwargs['execution_date']}"
        logging.error(results)

    kwargs['ti'].xcom_push(key='etl_results', value="<b>{}</b>: {}"
                           .format(table_name_, results))

def get_restapi_pto_plans(table_name_, **kwargs):
    ultipro = APIFactory.get_api("ultipro-restapis")
    snowflake_dbhandle = DatabaseFactory.get_database("snowflake", sqlalchemy_=True)
    snowflake_dbhandle.connect(kwargs["snowflake_warehouse"],
                               kwargs["snowflake_db"],
                               kwargs["snowflake_schema"],
                               kwargs["snowflake_role"],
                               kwargs["snowflake_user"],
                               kwargs["snowflake_pwd"],
                               kwargs["snowflake_account"], airflow_=True)

    logging.info("Grabbing PTO Plan data.")
    df = ultipro.get_pto_plans()
    logging.info("Attempting to write to snowflake")
    if not isinstance(df, type(None)):
        results = df.write_dataframe_to_database(
            snowflake_dbhandle, table_name_, 'ULTIPRO', upsert_=True)
        logging.info("Done writing to Snowflake!")
    else:
        results = f"Got no data for this run {kwargs['execution_date']}"
        logging.error(results)

    kwargs['ti'].xcom_push(key='etl_results', value="<b>{}</b>: {}"
                           .format(table_name_, results))

def get_restapi_person_details(table_name_, **kwargs):
    ultipro = APIFactory.get_api("ultipro-restapis")
    snowflake_dbhandle = DatabaseFactory.get_database("snowflake", sqlalchemy_=True)
    snowflake_dbhandle.connect(kwargs["snowflake_warehouse"],
                               kwargs["snowflake_db"],
                               kwargs["snowflake_schema"],
                               kwargs["snowflake_role"],
                               kwargs["snowflake_user"],
                               kwargs["snowflake_pwd"],
                               kwargs["snowflake_account"], airflow_=True)

    logging.info("Grabbing Person Details data.")
    df = ultipro.get_person_details(filter_=ultipro.get_daily_filter(
            datetime_=kwargs["ds"], days_=3, type_=1, field_='dateTimeChanged'))
    logging.info("Attempting to write to snowflake")
    if not isinstance(df, type(None)):
        results = df.write_dataframe_to_database(
            snowflake_dbhandle, table_name_,'ULTIPRO', upsert_=True)
        logging.info("Done writing to Snowflake!")
    else:
        results = f"Got no data for this run {kwargs['execution_date']}"
        logging.error(results)

    kwargs['ti'].xcom_push(key='etl_results', value="<b>{}</b>: {}"
                           .format(table_name_, results))

def get_restapi_employee_job_history_details(table_name_, **kwargs):
    ultipro = APIFactory.get_api("ultipro-restapis")
    snowflake_dbhandle = DatabaseFactory.get_database("snowflake", sqlalchemy_=True)
    snowflake_dbhandle.connect(kwargs["snowflake_warehouse"],
                               kwargs["snowflake_db"],
                               kwargs["snowflake_schema"],
                               kwargs["snowflake_role"],
                               kwargs["snowflake_user"],
                               kwargs["snowflake_pwd"],
                               kwargs["snowflake_account"], airflow_=True)

    logging.info("Grabbing Person Details data.")
    df = ultipro.get_employee_job_history_details(filter_=ultipro.get_daily_filter(
            datetime_=kwargs["ds"], days_=3, type_=1, field_='dateTimeCreated'))
    logging.info("Attempting to write to snowflake")
    if not isinstance(df, type(None)):
        results = df.write_dataframe_to_database(
            snowflake_dbhandle, table_name_,'ULTIPRO', upsert_=True)
        logging.info("Done writing to Snowflake!")
    else:
        results = f"Got no data for this run {kwargs['execution_date']}"
        logging.error(results)

    kwargs['ti'].xcom_push(key='etl_results', value="<b>{}</b>: {}"
                           .format(table_name_, results))


def get_service_employee_employment_information(table_name_, **kwargs):

    ultipro = APIFactory.get_api("ultipro-services")

    snowflake_dbhandle = DatabaseFactory.get_database("snowflake", sqlalchemy_=True)
    snowflake_dbhandle.connect(kwargs["snowflake_warehouse"],
                               kwargs["snowflake_db"],
                               kwargs["snowflake_schema"],
                               kwargs["snowflake_role"],
                               kwargs["snowflake_user"],
                               kwargs["snowflake_pwd"],
                               kwargs["snowflake_account"], airflow_=True)

    logging.info(f"Grabbing {table_name_} data.")

    df = ultipro.get_employee_employement_information(query_=None)
    logging.info("Attempting to write to snowflake")
    if not isinstance(df, type(None)):
        results = df.write_dataframe_to_database(snowflake_dbhandle,
                                                 table_name_,'ULTIPRO',
                                                 upsert_=True)
        logging.info("Done writing to Snowflake!")
    else:
        results = f"Received no data for this run {kwargs['execution_date']}"
        logging.error(results)

    kwargs['ti'].xcom_push(key='etl_results',
                           value="<b>{}</b>: {}".format(table_name_, results))

def get_service_employee_compensation(table_name_, **kwargs):

    ultipro = APIFactory.get_api("ultipro-services")

    snowflake_dbhandle = DatabaseFactory.get_database("snowflake", sqlalchemy_=True)
    snowflake_dbhandle.connect(kwargs["snowflake_warehouse"],
                               kwargs["snowflake_db"],
                               kwargs["snowflake_schema"],
                               kwargs["snowflake_role"],
                               kwargs["snowflake_user"],
                               kwargs["snowflake_pwd"],
                               kwargs["snowflake_account"], airflow_=True)

    logging.info(f"Grabbing {table_name_} data.")

    df = ultipro.get_employee_compensation(query_=None)
    logging.info("Attempting to write to snowflake")
    if not isinstance(df, type(None)):
        results = df.write_dataframe_to_database(snowflake_dbhandle,
                                                 table_name_,'ULTIPRO',
                                                 upsert_=True)
        logging.info("Done writing to Snowflake!")
    else:
        results = f"Received no data for this run {kwargs['execution_date']}"
        logging.error(results)

    kwargs['ti'].xcom_push(key='etl_results',
                           value="<b>{}</b>: {}".format(table_name_, results))

def get_employee_latest_pay_statement(table_name_, **kwargs):

    ultipro = APIFactory.get_api("ultipro-services")

    snowflake_dbhandle = DatabaseFactory.get_database("snowflake", sqlalchemy_=True)
    snowflake_dbhandle.connect(kwargs["snowflake_warehouse"],
                               kwargs["snowflake_db"],
                               kwargs["snowflake_schema"],
                               kwargs["snowflake_role"],
                               kwargs["snowflake_user"],
                               kwargs["snowflake_pwd"],
                               kwargs["snowflake_account"], airflow_=True)

    logging.info(f"Grabbing {table_name_} data.")

    df = ultipro.get_employee_latest_pay_statement(query_=None)
    logging.info("Attempting to write to snowflake")
    if not isinstance(df, type(None)):
        results = df.write_dataframe_to_database(snowflake_dbhandle,
                                                 table_name_,'ULTIPRO',
                                                 upsert_=True)
        logging.info("Done writing to Snowflake!")
    else:
        results = f"Received no data for this run {kwargs['execution_date']}"
        logging.error(results)

    kwargs['ti'].xcom_push(key='etl_results',
                           value="<b>{}</b>: {}".format(table_name_, results))

def get_raas_report(table_name_, schema_file_name_, report_path_, **kwargs):

    ultipro = APIFactory.get_api("ultipro-raas")

    snowflake_dbhandle = DatabaseFactory.get_database("snowflake", sqlalchemy_=True)
    snowflake_dbhandle.connect(kwargs["snowflake_warehouse"],
                               kwargs["snowflake_db"],
                               kwargs["snowflake_schema"],
                               kwargs["snowflake_role"],
                               kwargs["snowflake_user"],
                               kwargs["snowflake_pwd"],
                               kwargs["snowflake_account"], airflow_=True)

    logging.info(f"Grabbing {table_name_} data.")

    df = ultipro.process_report_data(report_path_, schema_file_name_)
    logging.info("Attempting to write to snowflake")
    if not isinstance(df, type(None)):
        results = df.write_dataframe_to_database(snowflake_dbhandle,
                                                 table_name_,'ULTIPRO',
                                                 upsert_=False)
        logging.info("Done writing to Snowflake!")
    else:
        results = f"Received no data for this run {kwargs['execution_date']}"
        logging.error(results)

    kwargs['ti'].xcom_push(key='etl_results',
                           value="<b>{}</b>: {}".format(table_name_, results))


default_args = {
    'owner': 'glen.brazil@smiledirectclub.com',
    'start_date': datetime(2019, 10, 1),
    'email': AirflowHelpers.get_dag_emails("data-eng"),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ultipro',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(hours=1))


run_get_restapi_employment_details = PythonOperator(
    task_id='run_get_restapi_employment_details',
    provide_context=True,
    python_callable= get_restapi_employment_details,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'RESTAPI_EMPLOYMENT_DETAILS_PII'
    },
    dag=dag)

run_get_restapi_employee_changes = PythonOperator(
    task_id='run_get_restapi_employee_changes',
    provide_context=True,
    python_callable= get_restapi_employee_changes,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'RESTAPI_EMPLOYEE_CHANGES_PII'
    },
    dag=dag)

run_get_restapi_compensation_details = PythonOperator(
    task_id='run_get_restapi_compensation_details',
    provide_context=True,
    python_callable=get_restapi_compensation_details,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'RESTAPI_COMPENSATION_DETAILS_PII'
    },
    dag=dag)

run_get_restapi_pto_plans = PythonOperator(
    task_id='run_get_restapi_pto_plans',
    provide_context=True,
    python_callable=get_restapi_pto_plans,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'RESTAPI_PTO_PLANS_PII'
    },
    dag=dag)


run_get_restapi_person_details = PythonOperator(
    task_id='run_get_restapi_person_details',
    provide_context=True,
    python_callable=get_restapi_person_details,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'RESTAPI_PERSON_DETAILS_PII'
    },
    dag=dag)


run_get_restapi_employee_job_history_details = PythonOperator(
    task_id='run_get_restapi_employee_job_history_details',
    provide_context=True,
    python_callable=get_restapi_employee_job_history_details,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'RESTAPI_EMPLOYEE_JOB_HISTORY_DETAILS_PII'
    },
    dag=dag)


run_get_service_employee_employment_information = PythonOperator(
    task_id='run_get_service_employee_employment_information',
    provide_context=True,
    python_callable=get_service_employee_employment_information,
    start_date=START_DATE_NO_FILTER_TASKS,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'SERVICE_EMPLOYEE_EMPLOYMENT_INFORMATION_PII'
    },
    dag=dag)

run_get_service_employee_compensation = PythonOperator(
    task_id='run_get_service_employee_compensation',
    provide_context=True,
    python_callable=get_service_employee_compensation,
    start_date=START_DATE_NO_FILTER_TASKS,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'SERVICE_EMPLOYEE_COMPENSATION_PII'
    },
    dag=dag)

run_get_employee_latest_pay_statement = PythonOperator(
    task_id='run_get_employee_latest_pay_statement',
    provide_context=True,
    python_callable=get_employee_latest_pay_statement,
    start_date=START_DATE_NO_FILTER_TASKS,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'SERVICE_EMPLOYEE_PAY_STATEMENT_PII'
    },
    dag=dag)

run_get_raas_achievers_audit = PythonOperator(
    task_id='run_get_raas_achievers_audit',
    provide_context=True,
    python_callable=get_raas_report,
    start_date=START_DATE_NO_FILTER_TASKS,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'RAAS_ACHIEVERS_AUDIT_PII',
        'schema_file_name_': 'raas-achievers-audit',
        'report_path_': "/content/folder[@name='zzzCompany Folders']/folder[@name='SmileDirectClub, LLC']/folder[@name='UltiPro']/folder[@name='Shared Folders']/folder[@name='IT']/report[@name='RaaS']"
    },
    dag=dag)


email_status = EmailOperator(
    task_id='email_status',
    to=AirflowHelpers.get_dag_emails("data-eng"),
    retries=1,
    dag=dag,
    subject="AIRFLOW - COMPLETED: Ultipro",
    html_content="<h3>Ultipro data has been updated</h3><br>\
        <b>Tables updated:</b><br><br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_restapi_employment_details', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_restapi_employee_changes', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_restapi_compensation_details', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_restapi_pto_plans', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_restapi_person_details', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_restapi_employee_job_history_details', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_service_employee_employment_information', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_service_employee_compensation', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_employee_latest_pay_statement', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_raas_achievers_audit', key='etl_results') }}<br>"
)

(run_get_restapi_employment_details,
 run_get_restapi_employee_changes,
 run_get_restapi_compensation_details,
 run_get_restapi_pto_plans,
 run_get_restapi_person_details,
 run_get_restapi_employee_job_history_details,
 run_get_service_employee_employment_information,
 run_get_service_employee_compensation,
 run_get_employee_latest_pay_statement,
 run_get_raas_achievers_audit
) >> email_status


