import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from sdc_etl_libs.api_helpers.APIFactory import APIFactory
from sdc_etl_libs.aws_helpers.aws_helpers import AWSHelpers
from sdc_etl_libs.database_helpers.DatabaseFactory import DatabaseFactory
from sdc_etl_libs.airflow_helpers.AirflowHelpers import AirflowHelpers


if Variable.get("environment") == "development":
    db_creds = AWSHelpers.get_secrets(
        "snowflake/service_account/airflow")

    snowflake_user = db_creds["username"]
    snowflake_pwd = db_creds["password"]
    snowflake_account = db_creds["account"]
    snowflake_db = "DATAENGINEERING"
    snowflake_schema = "EXACTTARGET"
    snowflake_warehouse = db_creds["warehouse"]
    snowflake_role = "AIRFLOW_SERVICE_ROLE"

elif Variable.get("environment") == "production":
    db_creds = AWSHelpers.get_secrets(
        "snowflake/service_account/airflow")

    snowflake_user = db_creds["username"]
    snowflake_pwd = db_creds["password"]
    snowflake_account = db_creds["account"]
    snowflake_db = "MARKETING"
    snowflake_schema = "EXACTTARGET"
    snowflake_warehouse = db_creds["warehouse"]
    snowflake_role = "AIRFLOW_SERVICE_ROLE"


def get_events(**kwargs):
    exacttarget = APIFactory.get_api("exacttarget")
    snowflake_dbhandle = DatabaseFactory.get_database("snowflake",
                                                      sqlalchemy_=True)
    snowflake_dbhandle.connect(kwargs["snowflake_warehouse"],
                               kwargs["snowflake_db"],
                               kwargs["snowflake_schema"],
                               kwargs["snowflake_role"],
                               kwargs["snowflake_user"],
                               kwargs["snowflake_pwd"],
                               kwargs["snowflake_account"], airflow_=True
                               )
    logging.info("Grabbing Events data...")
    df = exacttarget.get_events(
        filter_=exacttarget.get_filter_for_last_n_minutes(
            'EventDate', 5, kwargs['execution_date']))
    logging.info("Attempting to write to Snowflake..")
    if not isinstance(df,type(None)):
        results = df.write_dataframe_to_database(snowflake_dbhandle, 'EVENTS_PII',
                                       'EXACTTARGET')
        logging.info("Done writing to Snowflake!")
    else:
        results = f"Got no data for this run {kwargs['execution_date']}"
        logging.error(results)

    kwargs['ti'].xcom_push(key='etl_results',
                           value="<b>EVENTS_PII</b>: {}".format(results))

def get_subscribers(**kwargs):
    exacttarget = APIFactory.get_api("exacttarget")
    snowflake_dbhandle = DatabaseFactory.get_database("snowflake",
                                                      sqlalchemy_=True)
    snowflake_dbhandle.connect(kwargs["snowflake_warehouse"],
                               kwargs["snowflake_db"],
                               kwargs["snowflake_schema"],
                               kwargs["snowflake_role"],
                               kwargs["snowflake_user"],
                               kwargs["snowflake_pwd"],
                               kwargs["snowflake_account"], airflow_=True
                               )
    logging.info("Grabbing Subscriber data...")
    df = exacttarget.get_subscribers(
        filter_=exacttarget.get_filter_for_last_n_minutes(
            'CreatedDate', 5, kwargs['execution_date']))
    logging.info("Attempting to write to Snowflake..")
    if not isinstance(df,type(None)):
        results = df.write_dataframe_to_database(snowflake_dbhandle, 'SUBSCRIBERS_PII',
                                       'EXACTTARGET')
        logging.info("Done writing to Snowflake!")
    else:
        results = f"Got no data for this run {kwargs['execution_date']}"
        logging.error(results)

    kwargs['ti'].xcom_push(key='etl_results',
                           value="<b>SUBSCRIBERS_PII</b>: {}".format(results))

def get_list_subscribers(**kwargs):
    exacttarget = APIFactory.get_api("exacttarget")
    snowflake_dbhandle = DatabaseFactory.get_database("snowflake",
                                                      sqlalchemy_=True)
    snowflake_dbhandle.connect(kwargs["snowflake_warehouse"],
                               kwargs["snowflake_db"],
                               kwargs["snowflake_schema"],
                               kwargs["snowflake_role"],
                               kwargs["snowflake_user"],
                               kwargs["snowflake_pwd"],
                               kwargs["snowflake_account"], airflow_=True
                               )
    logging.info("Grabbing new List Subscriber data...")
    df = exacttarget.get_list_subscribers(
        filter_=exacttarget.get_filter_for_last_n_minutes(
            'CreatedDate', 5, kwargs['execution_date']))
    logging.info("Attempting to write to Snowflake..")
    if not isinstance(df,type(None)):
        results = df.write_dataframe_to_database(snowflake_dbhandle, 'LIST_SUBSCRIBERS_PII',
                                       'EXACTTARGET')
        logging.info("Done writing to Snowflake!")
    else:
        results = f"Got no data for this run {kwargs['execution_date']}"
        logging.error(results)

    kwargs['ti'].xcom_push(key='etl_results',
                           value="<b>LIST_SUBSCRIBERS_PII</b>: {}"
                           .format(results))

def get_list_subscriber_updates(**kwargs):
    exacttarget = APIFactory.get_api("exacttarget")
    snowflake_dbhandle = DatabaseFactory.get_database("snowflake",
                                                      sqlalchemy_=True)
    snowflake_dbhandle.connect(kwargs["snowflake_warehouse"],
                               kwargs["snowflake_db"],
                               kwargs["snowflake_schema"],
                               kwargs["snowflake_role"],
                               kwargs["snowflake_user"],
                               kwargs["snowflake_pwd"],
                               kwargs["snowflake_account"], airflow_=True
                               )
    logging.info("Grabbing updated List Subscriber data...")
    df = exacttarget.get_list_subscribers(
        filter_=exacttarget.get_filter_for_last_n_minutes(
            'ModifiedDate', 5, kwargs['execution_date']))
    logging.info("Attempting to write to Snowflake..")
    if not isinstance(df,type(None)):
        results = df.write_dataframe_to_database(snowflake_dbhandle, 'LIST_SUBSCRIBERS_PII',
                                       'EXACTTARGET', upsert_=True)
        logging.info("Done writing to Snowflake!")
    else:
        results = f"Got no data for this run {kwargs['execution_date']}"
        logging.error(results)

    kwargs['ti'].xcom_push(key='etl_results',
                           value="<b>LIST_SUBSCRIBERS_PII (Updates)</b>: {}"
                           .format(results))

def get_sends(**kwargs):
    exacttarget = APIFactory.get_api("exacttarget")
    snowflake_dbhandle = DatabaseFactory.get_database("snowflake",
                                                      sqlalchemy_=True)
    snowflake_dbhandle.connect(kwargs["snowflake_warehouse"],
                               kwargs["snowflake_db"],
                               kwargs["snowflake_schema"],
                               kwargs["snowflake_role"],
                               kwargs["snowflake_user"],
                               kwargs["snowflake_pwd"],
                               kwargs["snowflake_account"], airflow_=True
                               )
    logging.info("Grabbing new List Subscriber data...")
    df = exacttarget.get_sends(
        filter_=exacttarget.get_filter_for_last_n_minutes(
            'CreatedDate', 5, kwargs['execution_date']))
    logging.info("Attempting to write to Snowflake..")
    if not isinstance(df, type(None)):
        results = df.write_dataframe_to_database(snowflake_dbhandle, 'SENDS_PII',
                                       'EXACTTARGET')
        logging.info("Done writing to Snowflake!")
    else:
        results = f"Got no data for this run {kwargs['execution_date']}"
        logging.error(results)

    kwargs['ti'].xcom_push(key='etl_results',
                           value="<b>SENDS_PII</b>: {}".format(results))

def get_send_updates(**kwargs):
    exacttarget = APIFactory.get_api("exacttarget")
    snowflake_dbhandle = DatabaseFactory.get_database("snowflake",
                                                      sqlalchemy_=True)
    snowflake_dbhandle.connect(kwargs["snowflake_warehouse"],
                               kwargs["snowflake_db"],
                               kwargs["snowflake_schema"],
                               kwargs["snowflake_role"],
                               kwargs["snowflake_user"],
                               kwargs["snowflake_pwd"],
                               kwargs["snowflake_account"], airflow_=True
                               )
    logging.info("Grabbing new List Subscriber data...")
    df = exacttarget.get_sends(
        filter_=exacttarget.get_filter_for_last_n_minutes(
            'ModifiedDate', 5, kwargs['execution_date']))
    logging.info("Attempting to write to Snowflake..")
    if not isinstance(df,type(None)):
        results = df.write_dataframe_to_database(snowflake_dbhandle, 'SENDS_PII',
                                       'EXACTTARGET', upsert_=True)
        logging.info("Done writing to Snowflake!")
    else:
        results = f"Got no data for this run {kwargs['execution_date']}"
        logging.error(results)

    kwargs['ti'].xcom_push(key='etl_results',
                           value="<b>SENDS_PII (Updates)</b>: {}".format(results))


default_args = {
    'owner': 'data.engineering',
    'start_date': datetime(2018, 1, 1),
    'email': AirflowHelpers.get_dag_emails("data-eng"),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl-exacttarget-api',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    dagrun_timeout=timedelta(hours=2)
)


run_exacttarget_events = PythonOperator(
    task_id='task_run_exacttarget_events',
    provide_context=True,
    python_callable=get_events,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account
    },
    dag=dag)

run_exacttarget_subscribers = PythonOperator(
    task_id='task_run_exacttarget_subscribers',
    provide_context=True,
    python_callable=get_subscribers,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account
    },
    dag=dag)

run_exacttarget_list_subscribers = PythonOperator(
    task_id='task_run_exacttarget_list_subscribers',
    provide_context=True,
    python_callable=get_list_subscribers,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account
    },
    dag=dag)

run_exacttarget_list_subscriber_updates = PythonOperator(
    task_id='task_run_exacttarget_list_subscriber_updates',
    provide_context=True,
    python_callable=get_list_subscriber_updates,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account
    },
    dag=dag)

run_exacttarget_sends = PythonOperator(
    task_id='task_run_exacttarget_sends',
    provide_context=True,
    python_callable=get_sends,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account
    },
    dag=dag)

run_exacttarget_send_updates = PythonOperator(
    task_id='task_run_exacttarget_send_updates',
    provide_context=True,
    python_callable=get_send_updates,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account
    },
    dag=dag)

email_completion = EmailOperator(
    task_id='email_complete_exacttarget',
    to=AirflowHelpers.get_dag_emails(dag.dag_id),
    retries=0,
    dag=dag,
    subject="AIRFLOW - COMPLETED: ExactTarget",
    html_content="<h3>ExactTarget data has been updated</h3><br>\
    <b>Tables updated:</b><br><br>\
    {{ task_instance.xcom_pull(""task_ids='task_run_exacttarget_events', key='etl_results') }}<br>\
    {{ task_instance.xcom_pull(""task_ids='task_run_exacttarget_subscribers', key='etl_results') }}<br>\
    {{ task_instance.xcom_pull(""task_ids='task_run_exacttarget_list_subscribers', key='etl_results') }}<br>\
    {{ task_instance.xcom_pull(""task_ids='task_run_exacttarget_list_subscriber_updates', key='etl_results') }}<br>\
    {{ task_instance.xcom_pull(""task_ids='task_run_exacttarget_sends', key='etl_results') }}<br>\
    {{ task_instance.xcom_pull(""task_ids='task_run_exacttarget_send_updates', key='etl_results') }}"
)

(
    run_exacttarget_events,
    run_exacttarget_subscribers,
    run_exacttarget_sends,
    run_exacttarget_send_updates,
    run_exacttarget_list_subscribers,
    run_exacttarget_list_subscriber_updates
) >> email_completion
