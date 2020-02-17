
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
    db_creds = AWSHelpers.get_secrets("snowflake/service_account/airflow")

    snowflake_user = db_creds["username"]
    snowflake_pwd = db_creds["password"]
    snowflake_account = db_creds["account"]
    snowflake_db = "DATAENGINEERING"
    snowflake_schema = "TIMECONTROL"
    snowflake_warehouse = db_creds["warehouse"]
    snowflake_role = '"AIRFLOW_SERVICE_ROLE"'

elif Variable.get("environment") == "production":
    db_creds = AWSHelpers.get_secrets("snowflake/service_account/airflow")

    snowflake_user = db_creds["username"]
    snowflake_pwd = db_creds["password"]
    snowflake_account = db_creds["account"]
    snowflake_db = "HRIS_DATA"
    snowflake_schema = "TIMECONTROL"
    snowflake_warehouse = db_creds["warehouse"]
    snowflake_role = '"AIRFLOW_SERVICE_ROLE"'

def get_max_key(db_handle_, key_json_path_, db_, db_schema_, db_table_):
    """
    Returns the max primary Key integer value for a TimeControl table in
    Snowflake. If Null is returned from table, Key value is set to 0.

    :param db_handle_: Snowflake database handle/client.
    :param key_json_path_: The Key JSON path that is used in the related API
        endpoint filtering. Example: Employee/Timesheets/Key
    :param db_: Snowflake database.
    :param db_schema_: Snowflake schema.
    :param db_table_: Snowflake table.
    :return: Key value as an Integer.
    """

    path = key_json_path_.replace('/',':')
    query = f'select coalesce(max({path}::int), 0) from "{db_}"."{db_schema_}"."{db_table_}";'
    db_handle_.execute_query(query, return_results_=True)
    for row in db_handle_.get_results():
        key = row[0]
    return key

def get_timecontrol_data(table_name_, data_schema_name_, endpoint_name_,
                         limit_=None, filter_by_date_=False,
                         filter_by_key_=False, upsert_=False, filter_days_=None,
                         filter_field_list_=None, filter_key_path_=None,
                         **kwargs):
    """
    Grabs, processes and writes TimeControl API data to Snowflake. Sends ETL
    results log to Airflow Xcoms.

    :param table_name_: Snowflake table name to write to.
    :param data_schema_name_: Name of the data schema file for use in SDCDataFrame
        schema setup (do not include the file extension).
    :param endpoint_name_: TimeControl API endpoint name that will be appened
        to the base URL.
    :param limit_: Int. Number of records to return per page.
    :param filter_by_date_: Boolean. If True, an OData spec filter will be generated
        for the API calls based on the Airflow execution date. Must provide
        filter_days_ & filter_field_list_. Default = False.
    :param filter_by_key_: Boolean. If True, an OData spec filter will be generated
        for the API calls based on the max Key value found in the database table
        (greater than or equal to that value). The Must provide filter_key_path_.
        Default = False.
    :param upsert_: Boolean. If True, data will be upserted/merged into table.
    :param filter_days_: Integer. Number of days to go back from Airflow execution
        date to generate filter Start Date (Execution Date will be End Date).
        Default = None.
    :param filter_field_list_: List of date-based API fields that will be used
        to generate date filter. When more than one field, each additional field
        with be added to filter with 'or'. Default = None.
    :param filter_key_path_: String. JSON path of Key in the API endpoint data.
        Default = None.
    :param kwargs: Airflow keyword arguments.
    :return: None
    """

    timecontrol = APIFactory.get_api("timecontrol")

    snowflake_dbhandle = DatabaseFactory.get_database("snowflake", sqlalchemy_=True)
    snowflake_dbhandle.connect(kwargs["snowflake_warehouse"],
                               kwargs["snowflake_db"],
                               kwargs["snowflake_schema"],
                               kwargs["snowflake_role"],
                               kwargs["snowflake_user"],
                               kwargs["snowflake_pwd"],
                               kwargs["snowflake_account"], airflow_=True)

    if filter_by_date_:
        if filter_field_list_ and filter_days_:
            filter = timecontrol.get_daily_filter(kwargs['execution_date'],
                filter_days_, filter_field_list_)

        else:
            raise Exception("Need to pass datetime_, days_ and field_ for Time"
                            "Management filter use")
    elif filter_by_key_:

        max_key = get_max_key(snowflake_dbhandle, filter_key_path_,
                              kwargs["snowflake_db"], kwargs["snowflake_schema"],
                              table_name_)

        filter = timecontrol.get_key_filter(start_key_=max_key,
                                            json_key_path_=filter_key_path_)

    else:
        filter = None

    logging.info(f"Grabbing {data_schema_name_} data...")
    df = timecontrol.get_data(data_schema_name_=data_schema_name_,
                              endpoint_=endpoint_name_,
                              filter_=filter,
                              limit_=limit_)

    logging.info("Attempting to write to Snowflake...")

    if not isinstance(df, type(None)):
        results = df.write_dataframe_to_database(
            snowflake_dbhandle, table_name_, 'TIMECONTROL', upsert_=upsert_)
        logging.info("Done writing to Snowflake!")
    else:
        results = f"Received no data for this run {kwargs['execution_date']}"
        logging.error(results)

    kwargs['ti'].xcom_push(key='etl_results', value="<b>{}</b>: {}"
                           .format(table_name_, results))

default_args = {
    'owner': 'trevor.wnuk@smiledirectclub.com',
    'start_date': datetime(2019, 10, 11),
    'email': AirflowHelpers.get_dag_emails("data-eng"),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'timecontrol-api',
    default_args=default_args,
    schedule_interval='00 11,18 * * *',
    dagrun_timeout=timedelta(hours=1))


run_get_employees = PythonOperator(
    task_id='run_get_employees',
    provide_context=True,
    python_callable= get_timecontrol_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'EMPLOYEES_PII',
        'data_schema_name_': 'employees',
        'endpoint_name_': 'employees',
        'limit_': 1000,
        'upsert_': True,
        'filter_by_date_': True,
        'filter_field_list_': ['LastModifiedAt'],
        'filter_days_': 3
    },
    dag=dag)

run_get_employee_revisions = PythonOperator(
    task_id='run_get_employee_revisions',
    provide_context=True,
    python_callable= get_timecontrol_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'EMPLOYEE_REVISIONS_PII',
        'data_schema_name_': 'employee-revisions',
        'endpoint_name_': 'employeerevisions',
        'limit_': 1000,
        'upsert_': True,
        'filter_by_date_': True,
        'filter_field_list_': ['LastModifiedAt'],
        'filter_days_': 3
    },
    dag=dag)

run_get_charges = PythonOperator(
    task_id='run_get_charges',
    provide_context=True,
    python_callable= get_timecontrol_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'CHARGES',
        'data_schema_name_': 'charges',
        'endpoint_name_': 'charges',
        'limit_': 1000,
        'upsert_': True,
        'filter_by_date_': True,
        'filter_field_list_': ['LastModifiedAt'],
        'filter_days_': 3
    },
    dag=dag)

run_get_charge_revisions = PythonOperator(
    task_id='run_get_charge_revisions',
    provide_context=True,
    python_callable= get_timecontrol_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'CHARGE_REVISIONS',
        'data_schema_name_': 'charge-revisions',
        'endpoint_name_': 'chargerevisions',
        'limit_': 1000,
        'upsert_': True,
        'filter_by_date_': True,
        'filter_field_list_': ['LastModifiedAt'],
        'filter_days_': 3
    },
    dag=dag)

run_get_projects = PythonOperator(
    task_id='run_get_projects',
    provide_context=True,
    python_callable= get_timecontrol_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'PROJECTS',
        'data_schema_name_': 'projects',
        'endpoint_name_': 'projects',
        'limit_': 1000,
        'upsert_': True,
        'filter_by_date_': True,
        'filter_field_list_': ['LastModifiedAt'],
        'filter_days_': 3
    },
    dag=dag)

run_get_resources = PythonOperator(
    task_id='run_get_resources',
    provide_context=True,
    python_callable= get_timecontrol_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'RESOURCES',
        'data_schema_name_': 'resources',
        'endpoint_name_': 'resources',
        'limit_': 1000,
        'upsert_': True,
        'filter_by_date_': True,
        'filter_field_list_': ['LastModifiedAt'],
        'filter_days_': 3
    },
    dag=dag)

run_get_users = PythonOperator(
    task_id='run_get_users',
    provide_context=True,
    python_callable= get_timecontrol_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'USERS_PII',
        'data_schema_name_': 'users',
        'endpoint_name_': 'users',
        'limit_': 1000,
        'upsert_': True,
        'filter_by_date_': True,
        'filter_field_list_': ['LastModifiedAt'],
        'filter_days_': 3
    },
    dag=dag)

run_get_wbs = PythonOperator(
    task_id='run_get_wbs',
    provide_context=True,
    python_callable= get_timecontrol_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'WBS',
        'data_schema_name_': 'wbs',
        'endpoint_name_': 'wbs',
        'limit_': 1000,
        'upsert_': True,
        'filter_by_date_': True,
        'filter_field_list_': ['LastModifiedAt'],
        'filter_days_': 3
    },
    dag=dag)

run_get_rates = PythonOperator(
    task_id='run_get_rates',
    provide_context=True,
    python_callable= get_timecontrol_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'RATES_PII',
        'data_schema_name_': 'rates',
        'endpoint_name_': 'rates',
        'limit_': 1000,
        'upsert_': True,
        'filter_by_date_': True,
        'filter_field_list_': ['LastModifiedAt'],
        'filter_days_': 3
    },
    dag=dag)

run_get_reports = PythonOperator(
    task_id='run_get_reports',
    provide_context=True,
    python_callable= get_timecontrol_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'REPORTS',
        'data_schema_name_': 'reports',
        'endpoint_name_': 'reports',
        'limit_': 1000,
        'upsert_': True
    },
    dag=dag)

run_get_languages = PythonOperator(
    task_id='run_get_languages',
    provide_context=True,
    python_callable= get_timecontrol_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'LANGUAGES',
        'data_schema_name_': 'languages',
        'endpoint_name_': 'languages',
        'limit_': 1000,
        'upsert_': True
    },
    dag=dag)

run_get_periods_pay = PythonOperator(
    task_id='run_get_periods_pay',
    provide_context=True,
    python_callable= get_timecontrol_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'PERIODS_PAY',
        'data_schema_name_': 'periods-pay',
        'endpoint_name_': 'periods/pay',
        'limit_': 1000,
        'upsert_': True,
        'filter_by_key_': True,
        'filter_key_path_': "Key"
    },
    dag=dag)

run_get_periods_timesheet = PythonOperator(
    task_id='run_get_periods_timesheet',
    provide_context=True,
    python_callable= get_timecontrol_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'PERIODS_TIMESHEET',
        'data_schema_name_': 'periods-timesheet',
        'endpoint_name_': 'periods/timesheet',
        'limit_': 1000,
        'upsert_': True,
        'filter_by_key_': True,
        'filter_key_path_': "Key"
    },
    dag=dag)

run_get_timesheets = PythonOperator(
    task_id='run_get_timesheets',
    provide_context=True,
    python_callable= get_timecontrol_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'TIMESHEETS_PII',
        'data_schema_name_': 'timesheets',
        'endpoint_name_': 'timesheets',
        'limit_': 1000,
        'upsert_': True,
        'filter_by_key_': True,
        'filter_key_path_': "Key"
    },
    dag=dag)

run_get_timesheets_details = PythonOperator(
    task_id='run_get_timesheets_details',
    provide_context=True,
    python_callable= get_timecontrol_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'TIMESHEETS_DETAILS_PII',
        'data_schema_name_': 'timesheets-details',
        'endpoint_name_': 'timesheets/details',
        'limit_': 1000,
        'upsert_': True,
        'filter_by_key_': True,
        'filter_key_path_': "Key"
    },
    dag=dag)

run_get_timesheets_posted = PythonOperator(
    task_id='run_get_timesheets_posted',
    provide_context=True,
    python_callable= get_timecontrol_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'TIMESHEETS_POSTED_PII',
        'data_schema_name_': 'timesheets-posted',
        'endpoint_name_': 'timesheets/posted',
        'limit_': 1000,
        'upsert_': True,
        'filter_by_key_': True,
        'filter_key_path_': "Key"
    },
    dag=dag)

run_get_timesheets_posted_details = PythonOperator(
    task_id='run_get_timesheets_posted_details',
    provide_context=True,
    python_callable= get_timecontrol_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'TIMESHEETS_POSTED_DETAILS_PII',
        'data_schema_name_': 'timesheets-posted-details',
        'endpoint_name_': 'timesheets/posted/details',
        'limit_': 1000,
        'upsert_': True,
        'filter_by_key_': True,
        'filter_key_path_': "Key"
    },
    dag=dag)


email_status = EmailOperator(
    task_id='email_status',
    to=AirflowHelpers.get_dag_emails("data-eng"),
    retries=1,
    dag=dag,
    subject="TimeControl: AIRFLOW completed",
    html_content="<h3>TimeControl data has been updated</h3><br>\
        <b>Tables updated:</b><br><br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_timesheets', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_timesheets_details', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_timesheets_posted', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_timesheets_posted_details', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_employees', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_employee_revisions', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_languages', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_charges', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_charge_revisions', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_projects', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_users', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_resources', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_rates', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_reports', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_periods_pay', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_periods_timesheet', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_wbs', key='etl_results') }}<br>"
)

(
 run_get_employees,
 run_get_employee_revisions,
 run_get_languages,
 run_get_charges,
 run_get_charge_revisions,
 run_get_projects,
 run_get_users,
 run_get_resources,
 run_get_wbs,
 run_get_reports,
 run_get_rates,
 run_get_timesheets,
 run_get_timesheets_details,
 run_get_timesheets_posted,
 run_get_timesheets_posted_details,
 run_get_periods_pay,
 run_get_periods_timesheet
) >> email_status

