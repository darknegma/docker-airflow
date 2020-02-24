
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from sdc_etl_libs.airflow_helpers.AirflowHelpers import AirflowHelpers
from sdc_etl_libs.sdc_data_exchange.SDCDataExchange import SDCDataExchange


if Variable.get("environment") == "development":
    customer_snowflake_data_to_experian_sftp_args = {"source_": "main_source", "sink_": "alternate_sink_0"}

elif Variable.get("environment") == "production":
    customer_snowflake_data_to_experian_sftp_args = {"source_": "main_source", "sink_": "alternate_sink_0"}


def customer_snowflake_data_to_experian_sftp(**kwargs):

    exchange = SDCDataExchange(
        "Experian/experian_customer_input", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Customer Snowflake data to Experian sftp", result, **kwargs)



default_args = {
    'owner': 'udai.shergill',
    'start_date': datetime(2025, 12, 23),
    'email': AirflowHelpers.get_dag_emails("data-eng"),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl-sdc-to-experian',
    default_args=default_args,
    schedule_interval='35 14 * * *',
    dagrun_timeout=timedelta(hours=2),
    max_active_runs=1
)

run_customer_snowflake_data_to_experian_sftp = PythonOperator(
    task_id='run_experian_data_s3_to_snowflake',
    provide_context=True,
    python_callable=customer_snowflake_data_to_experian_sftp,
    op_kwargs=customer_snowflake_data_to_experian_sftp_args,
    dag=dag)


generate_email = PythonOperator(
    task_id='generate_email',
    provide_context=True,
    op_kwargs={
            'etl_name_': "SDC Customers to EXPERIAN",
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

run_customer_snowflake_data_to_experian_sftp >> generate_email >> send_email