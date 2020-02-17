
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
from sdc_etl_libs.sdc_file_helpers.SDCFileHelpers import SDCFileHelpers


if Variable.get("environment") == "development":
    db_creds = AWSHelpers.get_secrets("snowflake/service_account/airflow")

    snowflake_user = db_creds["username"]
    snowflake_pwd = db_creds["password"]
    snowflake_account = db_creds["account"]
    snowflake_db = "DATAENGINEERING"
    snowflake_schema = "TRUEVAULT"
    snowflake_warehouse = db_creds["warehouse"]
    snowflake_role = "AIRFLOW_SERVICE_ROLE"

elif Variable.get("environment") == "production":
    db_creds = AWSHelpers.get_secrets("snowflake/service_account/airflow")

    snowflake_user = db_creds["username"]
    snowflake_pwd = db_creds["password"]
    snowflake_account = db_creds["account"]
    snowflake_db = "MEDICAL_DATA"
    snowflake_schema = "TRUEVAULT"
    snowflake_warehouse = db_creds["warehouse"]
    snowflake_role = "AIRFLOW_SERVICE_ROLE"


def get_truevault_dental_health_questionnaire(**kwargs):
    """
    Generates a list of TrueVault ids, grabs data and writes to Snowflake.
    :param kwargs: Keyword arguments needed for Snowflake connection.
    :return: None.
    """

    today = kwargs["execution_date"]
    end_date = (today + timedelta(days=1)).strftime('%Y-%m-%d')
    start_date = (today - timedelta(days=kwargs["days"])).strftime('%Y-%m-%d')

    truevault = APIFactory.get_api("truevault")
    snowflake_dbhandle = DatabaseFactory.get_database("snowflake", sqlalchemy_=True)
    snowflake_dbhandle.connect(kwargs["snowflake_warehouse"],
                               kwargs["snowflake_db"],
                               kwargs["snowflake_schema"],
                               kwargs["snowflake_role"],
                               kwargs["snowflake_user"],
                               kwargs["snowflake_pwd"],
                               kwargs["snowflake_account"],
                               airflow_=True)
    logging.info("Grabbing Dental Health Questionnaire data...")

    truevault.get_data_schema("TrueVault/dental-health-questionnaire")

    file_name = SDCFileHelpers.get_file_path(
        "sql", "TrueVault/SELECT_truevault_external_keys_query.sql")
    query = open(file_name).read().format(start_date, end_date)

    snowflake_dbhandle.execute_query(query, return_results_=True)
    keys = []
    for rows in snowflake_dbhandle.get_results():
        keys.append(rows[1])

    df = truevault.get_documents(document_key_list_=keys)

    logging.info("Attempting to write to Snowflake...")
    if not isinstance(df,type(None)):
        df.cleanse_column_names(style_="snowflake")
        results = df.write_dataframe_to_database(
            snowflake_dbhandle, 'DENTAL_HEALTH_QUESTIONNAIRE_PII', 'TRUEVAULT', upsert_=True)
        logging.info(f"Done writing to Snowflake. {results}")
    else:
        results = f"Got no data for this run {kwargs['execution_date']}"
        logging.error(results)

    kwargs['ti'].xcom_push(
        key='etl_results', value="<b>DENTAL_HEALTH_QUESTIONNAIRE_PII</b>: {}".format(results))


def generate_dental_health_questionnaire_model(**kwargs):
    """
    Generates a fact model based on the dental health questionnaire model.
    :param kwargs: Keyword arguments needed for Snowflake connection.
    :return: None.
    """

    file_name = SDCFileHelpers.get_file_path(
        "sql", f'TrueVault/CREATE_TABLE_fact_dental_health_questionnaire_pii_query.sql')
    query = open(file_name).read()

    logging.info("Running Dental Health Questionnaire model..")
    snowflake_dbhandle = DatabaseFactory.get_database(
        "snowflake", sqlalchemy_=True)
    snowflake_dbhandle.connect(kwargs["snowflake_warehouse"],
                               kwargs["snowflake_db"],
                               kwargs["snowflake_schema"],
                               kwargs["snowflake_role"],
                               kwargs["snowflake_user"],
                               kwargs["snowflake_pwd"],
                               kwargs["snowflake_account"],
                               airflow_=True)
    snowflake_dbhandle.execute_query(query, return_results_=True)
    for row in snowflake_dbhandle.get_results():
        results = row[0]
    logging.info(f"Done generating table. {results}")
    kwargs['ti'].xcom_push(
        key='etl_results', value="<b>FACT_DENTAL_HEALTH_QUESTIONNAIRE</b>: {}".format(results))


default_args = {
    'owner': 'data.engineering',
    'start_date': datetime(2019, 12, 18),
    'email': AirflowHelpers.get_dag_emails("data-eng"),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'etl-truevault',
    default_args=default_args,
    schedule_interval='30 11,17 * * *',
    dagrun_timeout=timedelta(hours=2)
)

run_get_truevault_dental_health_questionnaire = PythonOperator(
    task_id='run_get_truevault_dental_health_questionnaire',
    provide_context=True,
    python_callable=get_truevault_dental_health_questionnaire,
    op_kwargs={
        'days': 1,
        'snowflake_user': snowflake_user,
        'snowflake_pwd': snowflake_pwd,
        'snowflake_db': snowflake_db,
        'snowflake_schema': snowflake_schema,
        'snowflake_warehouse': snowflake_warehouse,
        'snowflake_role': snowflake_role,
        'snowflake_account': snowflake_account
    },
    dag=dag)

run_generate_dental_health_questionnaire_model = PythonOperator(
    task_id='run_generate_dental_health_questionnaire_model',
    provide_context=True,
    python_callable=generate_dental_health_questionnaire_model,
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
    task_id='email_complete_truevault',
    to=AirflowHelpers.get_dag_emails(dag.dag_id),
    retries=0,
    dag=dag,
    subject="AIRFLOW - COMPLETED: TrueVault",
    html_content="<h3>TrueVault data has been updated</h3><br>\
    <b>Tables updated:</b><br><br>\
    {{ task_instance.xcom_pull(""task_ids='run_get_truevault_dental_health_questionnaire', key='etl_results') }}<br>\
    {{ task_instance.xcom_pull(""task_ids='run_generate_dental_health_questionnaire_model', key='etl_results') }}"
)


run_get_truevault_dental_health_questionnaire >> run_generate_dental_health_questionnaire_model >> email_completion