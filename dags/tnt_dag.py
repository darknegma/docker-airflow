
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from sdc_etl_libs.airflow_helpers.AirflowHelpers import AirflowHelpers
from sdc_etl_libs.sdc_data_exchange.SDCDataExchange import SDCDataExchange

if Variable.get("environment") == "development":
    tracking_data_sftp_to_s3_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    tracking_data_s3_to_snowflake_args = {"source_": "alternate_source_0", "sink_": "SDC_sink_1_dev"}
    tracking_data_s3_to_salesforce_args = {"source_": "alternate_source_0", "sink_": "ExactTarget_sink_0"}

elif Variable.get("environment") == "production":
    tracking_data_sftp_to_s3_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    tracking_data_s3_to_snowflake_args = {"source_": "alternate_source_0", "sink_": "SDC_sink_1"}
    tracking_data_s3_to_salesforce_args = {"source_": "alternate_source_0", "sink_": "ExactTarget_sink_0"}


def tracking_data_sftp_to_s3(**kwargs):

    exchange = SDCDataExchange(
        "TNT/tracking-events", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Tracking Data from SDC SFTP to SDC S3", result, **kwargs)


def tracking_data_s3_to_snowflake(**kwargs):

    exchange = SDCDataExchange(
        "TNT/tracking-events", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Tracking Data from SDC S3 to Snowflake", result, **kwargs)


def tracking_data_s3_to_salesforce(**kwargs):

    exchange = SDCDataExchange(
        "TNT/tracking-events", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Tracking Data from SDC S3 to SalesForce SFTP", result, **kwargs)


default_args = {
    'owner': 'trevor.wnuk',
    'start_date': datetime(2019, 11, 25),
    'email': AirflowHelpers.get_dag_emails("data-eng"),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl-tnt',
    default_args=default_args,
    schedule_interval='15 * * * *',
    dagrun_timeout=timedelta(hours=2),
    max_active_runs=1,
    catchup=False
)

run_tracking_data_sftp_to_s3 = PythonOperator(
    task_id='run_tracking_data_sftp_to_s3',
    provide_context=True,
    python_callable=tracking_data_sftp_to_s3,
    op_kwargs=tracking_data_sftp_to_s3_args,
    dag=dag)

run_tracking_data_s3_to_snowflake = PythonOperator(
    task_id='run_tracking_data_s3_to_snowflake',
    provide_context=True,
    python_callable=tracking_data_s3_to_snowflake,
    op_kwargs=tracking_data_s3_to_snowflake_args,
    dag=dag)

run_tracking_data_s3_to_salesforce = PythonOperator(
    task_id='run_tracking_data_s3_to_salesforce',
    provide_context=True,
    python_callable=tracking_data_s3_to_salesforce,
    op_kwargs=tracking_data_s3_to_salesforce_args,
    dag=dag)

generate_email = PythonOperator(
    task_id='generate_email',
    provide_context=True,
    op_kwargs={
            'etl_name_': "TNT",
            'tasks_': [task.task_id for task in dag.tasks],
            'environment_': Variable.get("environment")
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

run_tracking_data_sftp_to_s3 >> (
    run_tracking_data_s3_to_salesforce,
    run_tracking_data_s3_to_snowflake
) >> generate_email >> send_email
