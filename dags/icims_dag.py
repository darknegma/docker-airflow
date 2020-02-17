
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from sdc_etl_libs.airflow_helpers.AirflowHelpers import AirflowHelpers
from sdc_etl_libs.sdc_data_exchange.SDCDataExchange import SDCDataExchange
from airflow.models import Variable

if Variable.get("environment") == "development":
    icims_persons_pii_sftp_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    icims_jobs_pii_sftp_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    icims_candidate_workflow_pii_sftp_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}


elif Variable.get("environment") == "production":
    icims_persons_pii_sftp_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    icims_jobs_pii_sftp_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    icims_candidate_workflow_pii_sftp_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}


def icims_persons_pii_sftp_to_db(**kwargs):

    exchange = SDCDataExchange(
        "iCIMS/persons-pii", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "iCIMS Persons data from SFTP to Snowflake", result, **kwargs)


def icims_jobs_pii_sftp_to_db(**kwargs):

    exchange = SDCDataExchange(
        "iCIMS/jobs-pii", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "iCIMS Jobs data from SFTP to Snowflake", result, **kwargs)


def icims_candidate_workflow_pii_sftp_to_db(**kwargs):

    exchange = SDCDataExchange(
        "iCIMS/candidate-workflow-pii", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "iCIMS Candidate Workflow data from SFTP to Snowflake", result,
        **kwargs)


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
    'etl-icims',
    default_args=default_args,
    schedule_interval='00 17 * * *',
    dagrun_timeout=timedelta(hours=2),
    max_active_runs=1,
    catchup=False
)

icims_persons_pii_sftp_to_db = PythonOperator(
    task_id='icims_persons_pii_sftp_to_db',
    provide_context=True,
    python_callable=icims_persons_pii_sftp_to_db,
    op_kwargs=icims_persons_pii_sftp_to_db_args,
    dag=dag)

icims_jobs_pii_sftp_to_db = PythonOperator(
    task_id='icims_jobs_pii_sftp_to_db',
    provide_context=True,
    python_callable=icims_jobs_pii_sftp_to_db,
    op_kwargs=icims_jobs_pii_sftp_to_db_args,
    dag=dag)

icims_candidate_workflow_pii_sftp_to_db = PythonOperator(
    task_id='icims_candidate_workflow_pii_sftp_to_db',
    provide_context=True,
    python_callable=icims_candidate_workflow_pii_sftp_to_db,
    op_kwargs=icims_candidate_workflow_pii_sftp_to_db_args,
    dag=dag)

generate_email = PythonOperator(
    task_id='generate_email',
    provide_context=True,
    op_kwargs={
            'etl_name_': "iCIMS",
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
    icims_persons_pii_sftp_to_db,
    icims_jobs_pii_sftp_to_db,
    icims_candidate_workflow_pii_sftp_to_db
) >> generate_email >> send_email
