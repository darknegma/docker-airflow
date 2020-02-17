
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
    snowflake_schema = "NEW_RELIC"
    snowflake_warehouse = db_creds["warehouse"]
    snowflake_role = '"AIRFLOW_SERVICE_ROLE"'

elif Variable.get("environment") == "production":
    db_creds = AWSHelpers.get_secrets("snowflake/service_account/airflow")

    snowflake_user = db_creds["username"]
    snowflake_pwd = db_creds["password"]
    snowflake_account = db_creds["account"]
    snowflake_db = "WEB_DATA"
    snowflake_schema = "NEW_RELIC"
    snowflake_warehouse = db_creds["warehouse"]
    snowflake_role = '"AIRFLOW_SERVICE_ROLE"'


def get_metrics_data(table_name_, data_schema_name_, filter_days_, filter_names_,
                     filter_values_, upsert_=False, **kwargs):
    """
    Processes New Relic API data and inserts into Snowflake.
    :param table_name_: Table name that data will be loaded into.
    :param data_schema_name_: Filename of JSON data schema.
    :param filter_days_: Days to set filter range as (with end date as
        the tasks's Airflow execution date)
    :param filter_names_: List of names for metrics requested.
    :param filter_values_: List of values for metrics requested.
    :param upsert_: Boolean. If true, data will be upserted into table based on
        merge keys is data schema. If false, data is inserted.
    :param kwargs: Keyword arguments.
    :return: None.
    """

    new_relic = APIFactory.get_api("new-relic")

    snowflake_dbhandle = DatabaseFactory.get_database("snowflake", sqlalchemy_=True)
    snowflake_dbhandle.connect(kwargs["snowflake_warehouse"],
                               kwargs["snowflake_db"],
                               kwargs["snowflake_schema"],
                               kwargs["snowflake_role"],
                               kwargs["snowflake_user"],
                               kwargs["snowflake_pwd"],
                               kwargs["snowflake_account"], airflow_=True)

    logging.info(f"Grabbing {data_schema_name_} data...")

    df = new_relic.get_metrics_data(data_schema_name_,
                                    date_filter_=new_relic.get_date_filter(
                                        kwargs['execution_date'],
                                        filter_days_),
                                    fields_filter_=new_relic.get_fields_filter(
                                        names_=filter_names_,
                                        values_=filter_values_)
                                    )

    logging.info("Attempting to write to Snowflake...")

    if not isinstance(df, type(None)):
        results = df.write_dataframe_to_database(
            snowflake_dbhandle, table_name_, 'NEW_RELIC', upsert_=upsert_)
        logging.info("Done writing to Snowflake!")
    else:
        results = f"Received no data for this run {kwargs['execution_date']}"
        logging.error(results)

    kwargs['ti'].xcom_push(key='etl_results', value="<b>{}</b>: {}"
                           .format(table_name_, results))


default_args = {
    'owner': 'trevor.wnuk@smiledirectclub.com',
    'start_date': datetime(2019, 11, 1),
    'email': AirflowHelpers.get_dag_emails("data-eng"),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'new-relic-api',
    default_args=default_args,
    schedule_interval='01 02 * * *',
    dagrun_timeout=timedelta(hours=1))

run_get_useragent_browser_performance = PythonOperator(
    task_id='run_get_useragent_browser_performance',
    provide_context=True,
    python_callable=get_metrics_data,
    op_kwargs={
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account,
        'table_name_': 'USERAGENT_BROWSER_PERFORMANCE',
        'data_schema_name_': 'useragent-browser-performance',
        'upsert_': True,
        'filter_names_': ["EndUser/UserAgent/Mobile/Browser",
                          "EndUser/UserAgent/Tablet/Browser",
                          "EndUser/UserAgent/Desktop/Browser"],
        'filter_values_': ["average_response_time",
                           "average_fe_response_time",
                           "average_be_response_time",
                           "average_network_time",
                           "error_percentage",
                           "calls_per_minute",
                           "requests_per_minute",
                           "call_count",
                           "min_response_time",
                           "max_response_time",
                           "total_network_time",
                           "network_time_percentage",
                           "total_fe_time",
                           "total_app_time",
                           "fe_time_percentage"],
        'filter_days_': 3
    },
    dag=dag)

email_status = EmailOperator(
    task_id='email_status',
    to=AirflowHelpers.get_dag_emails("data-eng"),
    retries=1,
    dag=dag,
    subject="New Relic: AIRFLOW completed",
    html_content="<h3>New Relic data has been updated</h3><br>\
        <b>Tables updated:</b><br><br>\
        {{ task_instance.xcom_pull(""task_ids='run_get_useragent_browser_performance', key='etl_results') }}<br>"
)

(
    run_get_useragent_browser_performance
) >> email_status
