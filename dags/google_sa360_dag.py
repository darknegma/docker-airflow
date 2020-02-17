
import logging
import json
from dateutil import parser
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from sdc_etl_libs.api_helpers.APIFactory import APIFactory
from sdc_etl_libs.aws_helpers.aws_helpers import AWSHelpers
from sdc_etl_libs.database_helpers.DatabaseFactory import DatabaseFactory
from sdc_etl_libs.airflow_helpers.AirflowHelpers import AirflowHelpers
from sdc_etl_libs.sdc_file_helpers.SDCFileHelpers import SDCFileHelpers


db_conn_args = dict()

if Variable.get("environment") == "development":
    db_creds = AWSHelpers.get_secrets(
        "snowflake/service_account/airflow")

    db_conn_args["snowflake_user"] = db_creds["username"]
    db_conn_args["snowflake_pwd"] = db_creds["password"]
    db_conn_args["snowflake_account"] = db_creds["account"]
    db_conn_args["snowflake_db"] = "DATAENGINEERING"
    db_conn_args["snowflake_schema"] = "GOOGLE_SEARCH_ADS_360"
    db_conn_args["snowflake_warehouse"] = db_creds["warehouse"]
    db_conn_args["snowflake_role"] = "AIRFLOW_SERVICE_ROLE"

elif Variable.get("environment") == "production":
    db_creds = AWSHelpers.get_secrets(
        "snowflake/service_account/airflow")

    db_conn_args["snowflake_user"] = db_creds["username"]
    db_conn_args["snowflake_pwd"] = db_creds["password"]
    db_conn_args["snowflake_account"] = db_creds["account"]
    db_conn_args["snowflake_db"] = "MARKETING"
    db_conn_args["snowflake_schema"] = "GOOGLE_SEARCH_ADS_360"
    db_conn_args["snowflake_warehouse"] = db_creds["warehouse"]
    db_conn_args["snowflake_role"] = "AIRFLOW_SERVICE_ROLE"


def get_report_request_body(file_name_):
    """
    Gets body or params data from file and returns as JSON object.
    :param file_name_: String. File path to file from metadata folder (including extension)
    :return: Dict. Param or body data as JSON object.
    """

    file_path = SDCFileHelpers.get_file_path("metadata", file_name_)
    file_data = json.loads(open(file_path).read())
    return file_data


def get_dates(num_lookback_days_, datetime_):
    """
    Calculates start and ending dates for report.
    :param num_lookback_days_: Int. Number of days to go back from start date to define end date.
    :param datetime_: Datetime object to calculate start date and end date from.
    :return: Strings. Start date and end date values as two variables.
    """

    if type(datetime_) == str:
        datetime_ = parser.parse(datetime_)

    end_date = (datetime_ + timedelta(days=1)).strftime('%Y-%m-%d')
    start_date = (datetime_ - timedelta(days=num_lookback_days_)).strftime('%Y-%m-%d')
    return start_date, end_date


def get_report_data(**kwargs):
    """
    Grabs report data from Google SearchAds 360.
    :param **kwargs: Various keyword arguments that are passed in via the Airflow task's PythonOperator op_kwargs:
        db_conn_args_: Dict. Snowflake database connection parameters.
        database_schema_name_: String. Database schema name to write to. Ex. "GOOGLE_SEARCH_ADS_360".
        database_table_name_: String. Database table name to write to. Ex. "REPORT_CONVERSION_EVENTS".
        num_lookback_days_: Int. Number of days to go back from start date pull data from.
        df_schema_name_: String. Name of dataframe schema file (without extension). Ex. "report-conversion-events".
        params_file_path_/body_file_path_: String. Path of params or body json (from metadata dir) for
            report request (with extension). Ex. "Vendors/SearchAds360/report-params-conversion-events.json".
            Only pass one or the other, not both.
    """

    # Initialize SearchAds360 instance
    searchads360 = APIFactory.get_api("searchads360")

    # Connect to Snowflake
    snowflake_dbhandle = DatabaseFactory.get_database("snowflake", sqlalchemy_=True)
    snowflake_dbhandle.connect(kwargs["db_conn_args_"]["snowflake_warehouse"],
                               kwargs["db_conn_args_"]["snowflake_db"],
                               kwargs["db_conn_args_"]["snowflake_schema"],
                               kwargs["db_conn_args_"]["snowflake_role"],
                               kwargs["db_conn_args_"]["snowflake_user"],
                               kwargs["db_conn_args_"]["snowflake_pwd"],
                               kwargs["db_conn_args_"]["snowflake_account"], airflow_=True)

    # Generate report dates and default params/body
    start_date, end_date = get_dates(
        num_lookback_days_=kwargs["num_lookback_days_"], datetime_=kwargs["execution_date"])
    params = None
    body = None

    # Add dates - if applicable - to param/body JSON
    if kwargs.get("params_file_path_"):
        params = get_report_request_body(kwargs["params_file_path_"])
        if params.get("startDate"):
            params["startDate"] = start_date
        if params.get("endDate"):
            params["endDate"] = end_date
        if params.get("changedAttributesSinceTimestamp"):
            params["changedAttributesSinceTimestamp"] = f'{start_date}T00:00:00Z'
        if params.get("changedMetricsSinceTimestamp"):
            params["changedMetricsSinceTimestamp"] = f'{start_date}T00:00:00Z'

    elif kwargs.get("body_file_path_"):
        body = get_report_request_body(kwargs["body_file_path_"])
        if body.get("timeRange"):
            if body.get("timeRange", {}).get("startDate"):
                body["timeRange"]["startDate"] = start_date
            if body.get("timeRange", {}).get("endDate"):
                body["timeRange"]["endDate"] = end_date

    logging.info("Grabbing Conversion Events data...")
    df = searchads360.get_report(schema_name_=kwargs["df_schema_name_"], params_=params, body_=body)

    logging.info("Attempting to write to Snowflake..")
    if not isinstance(df, type(None)):
        results = df.write_dataframe_to_database(
            sdc_database_handle_=snowflake_dbhandle,
            table_name_=kwargs["database_table_name_"],
            schema_name_=kwargs["database_schema_name_"],
            upsert_=True)
        logging.info("Done writing to Snowflake!")
    else:
        results = f"Got no data for this run: {kwargs['execution_date']}"
        logging.error(results)

    kwargs['ti'].xcom_push(
        key='etl_results', value=f'<b>{kwargs["database_table_name_"]}</b>: {results}')


default_args = {
    'owner': 'data.engineering',
    'start_date': datetime(2020, 1, 24),
    'email': AirflowHelpers.get_dag_emails("data-eng"),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'google-search-ads-360',
    default_args=default_args,
    schedule_interval='00 10 * * *',
    dagrun_timeout=timedelta(hours=2)
)


run_get_conversion_events = PythonOperator(
    task_id='run_get_conversion_events',
    provide_context=True,
    python_callable=get_report_data,
    op_kwargs={
        'db_conn_args_': db_conn_args,
        'database_schema_name_': "GOOGLE_SEARCH_ADS_360",
        'database_table_name_': "REPORT_CONVERSION_EVENTS",
        'num_lookback_days_': 3,
        'df_schema_name_': "report-conversion-events",
        'params_file_path_': "Vendors/SearchAds360/report-params-conversion-events.json",
        'body_file_path_': None
    },
    dag=dag)

run_get_keywords = PythonOperator(
    task_id='run_get_keywords',
    provide_context=True,
    python_callable=get_report_data,
    op_kwargs={
        'db_conn_args_': db_conn_args,
        'database_schema_name_': "GOOGLE_SEARCH_ADS_360",
        'database_table_name_': "REPORT_KEYWORDS",
        'num_lookback_days_': 3,
        'df_schema_name_': "report-keywords",
        'params_file_path_': "Vendors/SearchAds360/report-params-keywords.json",
        'body_file_path_': None
    },
    dag=dag)

run_get_ads = PythonOperator(
    task_id='run_get_ads',
    provide_context=True,
    python_callable=get_report_data,
    op_kwargs={
        'db_conn_args_': db_conn_args,
        'database_schema_name_': "GOOGLE_SEARCH_ADS_360",
        'database_table_name_': "REPORT_ADS",
        'num_lookback_days_': 3,
        'df_schema_name_': "report-ads",
        'params_file_path_': "Vendors/SearchAds360/report-params-ads.json",
        'body_file_path_': None
    },
    dag=dag)


email_completion = EmailOperator(
    task_id='email_complete_google_search_ads_360',
    to=AirflowHelpers.get_dag_emails(dag.dag_id),
    retries=0,
    dag=dag,
    subject="AIRFLOW - COMPLETED: Google Search Ads 360",
    html_content="<h3>Google Search Ads 360 data has been updated</h3><br>\
    <b>Tables updated:</b><br><br>\
    {{ task_instance.xcom_pull(""task_ids='run_get_ads', key='etl_results') }}<br>\
    {{ task_instance.xcom_pull(""task_ids='run_get_keywords', key='etl_results') }}<br>\
    {{ task_instance.xcom_pull(""task_ids='run_get_conversion_events', key='etl_results') }}"
)


(
    run_get_conversion_events,
    run_get_ads,
    run_get_keywords
) >> email_completion
