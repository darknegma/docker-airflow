from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import PythonOperator
from airflow.operators.email_operator import EmailOperator
from scripts.neustar_mta_ftp import NeustarMtaData
from sdc_etl_libs.airflow_helpers.AirflowHelpers import AirflowHelpers


default_args = {
    'owner': 'jeff.horn',
    'start_date': datetime(2019, 12, 13),
    'email': AirflowHelpers.get_dag_emails("data-eng"),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl-neustar-mta',
    default_args=default_args,
    schedule_interval='05 09 * * *',
    catchup=False,
    dagrun_timeout=timedelta(hours=2))

t_neustar_mta = PythonOperator(
    task_id='run_etl_neustar_mta',
    provide_context=True,
    python_callable=NeustarMtaData.neustar_mta_to_ftp,
    dag=dag)

t_email_completion = EmailOperator(
    task_id='email_etl_neustar_mta',
    to=AirflowHelpers.get_dag_emails(dag.dag_id),
    retries=0,
    dag=dag,
    subject="AIRFLOW - COMPLETED: Neustar MTA FTP Send",
    html_content="<h3>Neustar MTA transfer to FTP Site has\
        <font color=\"green\"> completed.</font></h3><br>\
        {{ task_instance.xcom_pull(task_ids='run_etl_neustar_mta', \
        key='etl_results') }}")

t_neustar_mta.set_downstream(t_email_completion)
