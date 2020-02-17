
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import PythonOperator
from airflow.operators.email_operator import EmailOperator
from scripts.neustar_email_data_ftp import NeustarEmailData
from sdc_etl_libs.airflow_helpers.AirflowHelpers import AirflowHelpers


default_args = {
    'owner': 'trevor.wnuk',
    'start_date': datetime(2019, 5, 30),
    'email': AirflowHelpers.get_dag_emails("data-eng"),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'etl-neustar-email-data',
    default_args=default_args,
    schedule_interval='15 15 * * *',
    dagrun_timeout=timedelta(hours=2))


t_neustar_email_data = PythonOperator(
    task_id='run_etl_neustar_email_data',
    provide_context=True,
    python_callable=NeustarEmailData.neustar_snowflake_to_ftp,
    dag=dag)


t_email_completion = EmailOperator(
    task_id='email_etl_neustar_email_data',
    to=AirflowHelpers.get_dag_emails(dag.dag_id),
    retries=0,
    dag=dag,
    subject="AIRFLOW - COMPLETED: Neustar E-mail Data FTP Send",
    html_content="<h3>Neustar E-mail Data to FTP Site has <font color=\"green\">completed.</font></h3><br>\
    {{ task_instance.xcom_pull(task_ids='run_etl_neustar_email_data', key='etl_results') }}")

t_neustar_email_data.set_downstream(t_email_completion)
