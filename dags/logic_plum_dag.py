
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from sdc_etl_libs.airflow_helpers.AirflowHelpers import AirflowHelpers
from sdc_etl_libs.sdc_data_exchange.SDCDataExchange import SDCDataExchange
from airflow.models import Variable


if Variable.get("environment") == "development":
    scan_show_rates_to_sftp_args = {"source_": "main_source", "sink_": "ExactTarget_sink_0"}
    scan_show_rates_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    scan_show_rates_btft_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    scan_show_rates_bts_country_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}

elif Variable.get("environment") == "production":
    scan_show_rates_to_sftp_args = {"source_": "main_source", "sink_": "ExactTarget_sink_0"}
    scan_show_rates_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    scan_show_rates_btft_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    scan_show_rates_bts_country_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}


def scan_show_rates_to_sftp(**kwargs):

    exchange = SDCDataExchange(
        "Logic_Plum/book-to-show-scores", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Scan Show Rates from S3 to SFTP", result, **kwargs)


def scan_show_rates_to_db(**kwargs):

    exchange = SDCDataExchange(
        "Logic_Plum/book-to-show-scores", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Scan Show Rates from S3 to Snowflake", result, **kwargs)


def scan_show_rates_btft_to_db(**kwargs):

    exchange = SDCDataExchange(
        "Logic_Plum/book-to-show-scores-btft", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Scan Show Rates BTFT from S3 to Snowflake", result, **kwargs)


def scan_show_rates_bts_country_to_db(**kwargs):

    exchange = SDCDataExchange(
        "Logic_Plum/book-to-show-scores-bts-country", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Scan Show Rates BTS Country from S3 to Snowflake", result, **kwargs)


default_args = {
    'owner': 'trevor.wnuk',
    'start_date': datetime(2019, 10, 25),
    'email': AirflowHelpers.get_dag_emails("data-eng"),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl-logic-plum',
    default_args=default_args,
    schedule_interval='0 * * * *',
    dagrun_timeout=timedelta(hours=2),
    max_active_runs=1,
    catchup=False
)


run_scan_show_rates_s3_to_sftp = PythonOperator(
    task_id='run_scan_show_rates_s3_to_sftp',
    provide_context=True,
    python_callable=scan_show_rates_to_sftp,
    op_kwargs=scan_show_rates_to_sftp_args,
    dag=dag)

run_scan_show_rates_s3_to_db = PythonOperator(
    task_id='run_scan_show_rates_s3_to_db',
    provide_context=True,
    python_callable=scan_show_rates_to_db,
    op_kwargs=scan_show_rates_to_db_args,
    dag=dag)

run_scan_show_rates_btft_s3_to_db = PythonOperator(
    task_id='run_scan_show_rates_btft_s3_to_db',
    provide_context=True,
    python_callable=scan_show_rates_btft_to_db,
    op_kwargs=scan_show_rates_btft_to_db_args,
    dag=dag)

run_scan_show_rates_bts_country_s3_to_db = PythonOperator(
    task_id='run_scan_show_rates_bts_country_s3_to_db',
    provide_context=True,
    python_callable=scan_show_rates_bts_country_to_db,
    op_kwargs=scan_show_rates_bts_country_to_db_args,
    dag=dag)

generate_email = PythonOperator(
    task_id='generate_email',
    provide_context=True,
    op_kwargs={
            'etl_name_': "Logic Plum",
            'tasks_': [task.task_id for task in dag.tasks]
        },
    python_callable=AirflowHelpers.generate_data_exchange_email,
    dag=dag)

send_email = EmailOperator(
    task_id='send_email',
    to=AirflowHelpers.get_dag_emails(dag.dag_id),
    retries=0,
    dag=dag,
    subject="{{ task_instance.xcom_pull(task_ids='generate_email', key='email_subject') }}",
    html_content="{{ task_instance.xcom_pull(task_ids='generate_email', key='email_body') }}"
)

(
    run_scan_show_rates_s3_to_sftp,
    run_scan_show_rates_s3_to_db,
    run_scan_show_rates_btft_s3_to_db,
    run_scan_show_rates_bts_country_s3_to_db
) >> generate_email >> send_email
