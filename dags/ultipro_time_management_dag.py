
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


def get_time_management_data(table_name_, filter_=False, **kwargs):

    ultipro = APIFactory.get_api("ultipro-timemanagement")

    if filter_ == True:
        if kwargs["filter_field_list_"] and kwargs["filter_days_"]:

            filter = ultipro.get_daily_filter(
                kwargs['execution_date'],
                kwargs["filter_days_"],
                kwargs["filter_field_list_"])

        else:
            raise Exception("Need to pass datetime_, days_ and field_ for Time"
                            "Management filter use")
    else:
        filter = None

    snowflake_dbhandle = DatabaseFactory.get_database("snowflake", sqlalchemy_=True)
    snowflake_dbhandle.connect(kwargs["snowflake_warehouse"],
                               kwargs["snowflake_db"],
                               kwargs["snowflake_schema"],
                               kwargs["snowflake_role"],
                               kwargs["snowflake_user"],
                               kwargs["snowflake_pwd"],
                               kwargs["snowflake_account"], airflow_=True)

    logging.info(f"Grabbing {table_name_} data.")
    df = ultipro.get_data_from_endpoint(kwargs["schema_name_"],
                                        kwargs["endpoint_name_"],
                                        filter)
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


default_args = {
    'owner': 'glen.brazil@smiledirectclub.com',
    'start_date': datetime(2020, 1, 7),
    'email': AirflowHelpers.get_dag_emails("data-eng"),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ultipro-time-management',
    default_args=default_args,
    schedule_interval='0 0,6,12 * * *',
    dagrun_timeout=timedelta(hours=1))


run_get_time_management_access_groups = PythonOperator(
    task_id='run_get_time_management_access_groups',
    provide_context=True,
    python_callable=get_time_management_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'TIME_MANAGEMENT_ACCESS_GROUPS',
        'schema_name_': 'access-group',
        'endpoint_name_': 'AccessGroup'
    },
    dag=dag)

run_get_time_management_org_level_1 = PythonOperator(
    task_id='run_get_time_management_org_level_1',
    provide_context=True,
    python_callable=get_time_management_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'TIME_MANAGEMENT_ORG_LEVEL_1',
        'schema_name_': 'org-level-1',
        'endpoint_name_': 'OrgLevel1'
    },
    dag=dag)

run_get_time_management_org_level_2 = PythonOperator(
    task_id='run_get_time_management_org_level_2',
    provide_context=True,
    python_callable=get_time_management_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'TIME_MANAGEMENT_ORG_LEVEL_2',
        'schema_name_': 'org-level-2',
        'endpoint_name_': 'OrgLevel2'
    },
    dag=dag)

run_get_time_management_org_level_3 = PythonOperator(
    task_id='run_get_time_management_org_level_3',
    provide_context=True,
    python_callable=get_time_management_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'TIME_MANAGEMENT_ORG_LEVEL_3',
        'schema_name_': 'org-level-3',
        'endpoint_name_': 'OrgLevel3'
    },
    dag=dag)

run_get_time_management_org_level_4 = PythonOperator(
    task_id='run_get_time_management_org_level_4',
    provide_context=True,
    python_callable=get_time_management_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'TIME_MANAGEMENT_ORG_LEVEL_4',
        'schema_name_': 'org-level-4',
        'endpoint_name_': 'OrgLevel4'
    },
    dag=dag)

run_get_time_management_employee = PythonOperator(
    task_id='run_get_time_management_employee',
    provide_context=True,
    python_callable=get_time_management_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'TIME_MANAGEMENT_EMPLOYEE_PII',
        'schema_name_': 'employee',
        'endpoint_name_': 'Employee'
    },
    dag=dag)

run_get_time_management_job = PythonOperator(
    task_id='run_get_time_management_job',
    provide_context=True,
    python_callable=get_time_management_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'TIME_MANAGEMENT_JOB',
        'schema_name_': 'job',
        'endpoint_name_': 'Job'
    },
    dag=dag)

run_get_time_management_location = PythonOperator(
    task_id='run_get_time_management_location',
    provide_context=True,
    python_callable=get_time_management_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'TIME_MANAGEMENT_LOCATION',
        'schema_name_': 'location',
        'endpoint_name_': 'Location'
    },
    dag=dag)

run_get_time_management_time = PythonOperator(
    task_id='run_get_time_management_time',
    provide_context=True,
    python_callable=get_time_management_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'TIME_MANAGEMENT_TIME_PII',
        'schema_name_': 'time',
        'endpoint_name_': 'Time',
        'filter_': True,
        'filter_field_list_': ['WorkDate', 'AdjustmentDate'],
        'filter_days_': 1
    },
    dag=dag)

run_get_time_management_reason = PythonOperator(
    task_id='run_get_time_management_reason',
    provide_context=True,
    python_callable=get_time_management_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'TIME_MANAGEMENT_REASON',
        'schema_name_': 'reason',
        'endpoint_name_': 'Reason'
    },
    dag=dag)

run_get_time_management_paycode = PythonOperator(
    task_id='run_get_time_management_paycode',
    provide_context=True,
    python_callable=get_time_management_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'TIME_MANAGEMENT_PAYCODE',
        'schema_name_': 'paycode',
        'endpoint_name_': 'Paycode'
    },
    dag=dag)

run_get_time_management_paygroup = PythonOperator(
    task_id='run_get_time_management_paygroup',
    provide_context=True,
    python_callable=get_time_management_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'TIME_MANAGEMENT_PAYGROUP',
        'schema_name_': 'paygroup',
        'endpoint_name_': 'Paygroup'
    },
    dag=dag)

run_get_time_management_project = PythonOperator(
    task_id='run_get_time_management_project',
    provide_context=True,
    python_callable=get_time_management_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'TIME_MANAGEMENT_PROJECT',
        'schema_name_': 'project',
        'endpoint_name_': 'Project'
    },
    dag=dag)

run_get_time_management_shift = PythonOperator(
    task_id='run_get_time_management_shift',
    provide_context=True,
    python_callable=get_time_management_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'TIME_MANAGEMENT_SHIFT',
        'schema_name_': 'shift',
        'endpoint_name_': 'Shift'
    },
    dag=dag)

run_get_time_management_shiftdet = PythonOperator(
    task_id='run_get_time_management_shiftdet',
    provide_context=True,
    python_callable=get_time_management_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'TIME_MANAGEMENT_SHIFTDET',
        'schema_name_': 'shiftdet',
        'endpoint_name_': 'ShiftDet'
    },
    dag=dag)


run_get_time_management_schedule_request = PythonOperator(
    task_id='run_get_time_management_schedule_request',
    provide_context=True,
    python_callable=get_time_management_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'TIME_MANAGEMENT_SCHEDULE_REQUEST',
        'schema_name_': 'schedule-request',
        'endpoint_name_': 'ScheduleRequest',
        'filter_': True,
        'filter_field_list_': ['Submitted'],
        'filter_days_': 1
    },
    dag=dag)

run_get_time_management_timesheet = PythonOperator(
    task_id='run_get_time_management_timesheet',
    provide_context=True,
    python_callable=get_time_management_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'TIME_MANAGEMENT_TIMESHEET',
        'schema_name_': 'timesheet',
        'endpoint_name_': 'Timesheet',
        'filter_': True,
        'filter_field_list_': ['SysUpdate', 'Starts'],
        'filter_days_': 1
    },
    dag=dag)


email_status = EmailOperator(
    task_id='email_status',
    to=AirflowHelpers.get_dag_emails("data-eng"),
    retries=1,
    dag=dag,
    subject="AIRFLOW - COMPLETED: Ultipro Time Management",
    html_content="<h3>Ultipro Time Management data has been updated</h3><br>\
        <b>Tables updated:</b><br><br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_time_management_access_groups', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_time_management_org_level_1', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_time_management_org_level_2', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_time_management_org_level_3', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_time_management_org_level_4', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_time_management_employee', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_time_management_job', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_time_management_location', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_time_management_time', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_time_management_reason', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_time_management_paycode', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_time_management_paygroup', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_time_management_project', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_time_management_shift', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_time_management_shiftdet', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_time_management_schedule_request', key='etl_results') }}<br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_time_management_timesheet', key='etl_results') }}<br>"
)

(
 run_get_time_management_access_groups,
 run_get_time_management_org_level_1,
 run_get_time_management_org_level_2,
 run_get_time_management_org_level_3,
 run_get_time_management_org_level_4,
 run_get_time_management_employee,
 run_get_time_management_job,
 run_get_time_management_location,
 run_get_time_management_time,
 run_get_time_management_reason,
 run_get_time_management_paycode,
 run_get_time_management_paygroup,
 run_get_time_management_project,
 run_get_time_management_shift,
 run_get_time_management_shiftdet,
 run_get_time_management_schedule_request,
 run_get_time_management_timesheet
) >> email_status


