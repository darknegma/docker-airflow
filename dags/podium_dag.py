
import json
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
    snowflake_schema = "PODIUM"
    snowflake_warehouse = db_creds["warehouse"]
    snowflake_role = "AIRFLOW_SERVICE_ROLE"

elif Variable.get("environment") == "production":
    db_creds = AWSHelpers.get_secrets(
        "snowflake/service_account/airflow")

    snowflake_user = db_creds["username"]
    snowflake_pwd = db_creds["password"]
    snowflake_account = db_creds["account"]
    snowflake_db = "MARKETING"
    snowflake_schema = "PODIUM"
    snowflake_warehouse = db_creds["warehouse"]
    snowflake_role = "AIRFLOW_SERVICE_ROLE"


def get_locations(**kwargs):

    table_name = "LOCATIONS"

    podium = APIFactory.get_api("podium")
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
    logging.info("Grabbing Locations data...")

    df = podium.get_locations()

    Variable.set("podium_locations_list", df.df["LOCATIONID"].to_list())

    logging.info("Attempting to write to Snowflake..")

    if not isinstance(df, type(None)):
        results = df.write_dataframe_to_database(snowflake_dbhandle, table_name,
                                       'PODIUM', upsert_=True, dedupe_=True)
        logging.info("Done writing to Snowflake!")
    else:
        logging.error(f"Received no data for this run {kwargs['execution_date']}")

    kwargs['ti'].xcom_push(key='etl_results', value="<b>{}</b>: {}"
                           .format(table_name, results))

def get_reviews(**kwargs):

    table_name = 'REVIEWS_PII'

    podium = APIFactory.get_api("podium")
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
    logging.info("Grabbing Reviews data...")

    locations = json.loads(Variable.get("podium_locations_list"))
    df = podium.get_reviews(locations, filter_=podium.get_date_range_filter(
        kwargs['execution_date'], 3))

    logging.info("Attempting to write to Snowflake..")

    if not isinstance(df, type(None)):
        results = df.write_dataframe_to_database(snowflake_dbhandle, table_name,
                                       'PODIUM', upsert_=True, dedupe_=True)
        logging.info("Done writing to Snowflake!")
    else:
        logging.error(f"Received no data for this run {kwargs['execution_date']}")

    kwargs['ti'].xcom_push(key='etl_results', value="<b>{}</b>: {}"
                           .format(table_name, results))

def get_invites(**kwargs):

    table_name = "INVITES_PII"

    podium = APIFactory.get_api("podium")
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
    logging.info("Grabbing Invites data...")

    locations = json.loads(Variable.get("podium_locations_list"))
    df = podium.get_invites(locations,
                            filter_=podium.get_date_range_filter(
                                kwargs['execution_date'], 3))

    logging.info("Attempting to write to Snowflake..")

    if not isinstance(df, type(None)):
        results = df.write_dataframe_to_database(snowflake_dbhandle, table_name,
                                       'PODIUM', upsert_=True, dedupe_=True)
        logging.info("Done writing to Snowflake!")
    else:
        logging.error(f"Received no data for this run {kwargs['execution_date']}")

    kwargs['ti'].xcom_push(key='etl_results', value="<b>{}</b>: {}"
                           .format(table_name, results))


default_args = {
    'owner': 'data.engineering',
    'start_date': datetime(2019, 9, 23),
    'email': AirflowHelpers.get_dag_emails("data-eng"),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl-podium-api',
    default_args=default_args,
    schedule_interval='00 11,18 * * *',
    dagrun_timeout=timedelta(hours=2)
)

run_podium_locations = PythonOperator(
    task_id='task_run_podium_locations',
    provide_context=True,
    python_callable=get_locations,
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

run_podium_reviews = PythonOperator(
    task_id='task_run_podium_reviews',
    provide_context=True,
    python_callable=get_reviews,
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

run_podium_invites = PythonOperator(
    task_id='task_run_podium_invites',
    provide_context=True,
    python_callable=get_invites,
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
    task_id='email_completion',
    to=AirflowHelpers.get_dag_emails(dag.dag_id),
    retries=0,
    dag=dag,
    subject="AIRFLOW - COMPLETED: Podium",
    html_content="<h3>Podium data has been updated</h3><br>\
    <b>Tables updated:</b><br><br>\
    {{ task_instance.xcom_pull(""task_ids='task_run_podium_locations', key='etl_results') }}<br>\
    {{ task_instance.xcom_pull(""task_ids='task_run_podium_reviews', key='etl_results') }}<br>\
    {{ task_instance.xcom_pull(""task_ids='task_run_podium_invites', key='etl_results') }}<br>"
)

# Locations needs to run first as the Reviews & Invites will be using the returned
# LOCATIONIDs to make their API calls.
run_podium_locations >> (
    run_podium_reviews,
    run_podium_invites
) >> email_completion
