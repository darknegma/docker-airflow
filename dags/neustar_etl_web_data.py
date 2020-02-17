
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import PythonOperator
from airflow.operators.email_operator import EmailOperator
from scripts.neustar_web_data_ftp import NeustarWebData
from sdc_etl_libs.airflow_helpers.AirflowHelpers import AirflowHelpers


default_args = {
    'owner': 'trevor.wnuk',
    'start_date': datetime(2019, 5, 5),
    'email': AirflowHelpers.get_dag_emails("data-eng"),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'etl-neustar-web-data',
    default_args=default_args,
    schedule_interval='05 15 * * *',
    dagrun_timeout=timedelta(hours=2))

t_neustar_web_data = PythonOperator(
    task_id='run_etl_neustar_web_data',
    provide_context=True,
    python_callable=NeustarWebData.neustar_snowflake_to_ftp,
    dag=dag)


t_email_completion = EmailOperator(
    task_id='email_etl_neustar_web_data',
    to=AirflowHelpers.get_dag_emails(dag.dag_id),
    retries=0,
    dag=dag,
    subject="AIRFLOW - COMPLETED: Neustar Web Data FTP Send",
    html_content="<h3>Neustar Web Data to FTP Site has\
        <font color=\"green\"> completed.</font></h3><br>\
        {{ task_instance.xcom_pull(task_ids='run_etl_neustar_web_data', \
        key='etl_results') }}")


t_neustar_web_data.set_downstream(t_email_completion)
