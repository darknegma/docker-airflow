
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import PythonOperator
from airflow.operators.email_operator import EmailOperator
from scripts.tealium_statuses_s3 import TealiumStatusData
from sdc_etl_libs.airflow_helpers.AirflowHelpers import AirflowHelpers


default_args = {
    'owner': 'trevor.wnuk',
    'start_date': datetime(2019, 7, 7),
    'email': AirflowHelpers.get_dag_emails("data-eng"),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=60)}


dag = DAG(
    'etl-tealium-status-data',
    default_args=default_args,
    schedule_interval='0 */2 * * *',
    dagrun_timeout=timedelta(hours=2))

t_tealium_status_data = PythonOperator(
    task_id='run_etl_tealium_status_data',
    provide_context=True,
    python_callable=TealiumStatusData.tealium_snowflake_to_s3,
    dag=dag,
    op_kwargs={'file_prefix_': 'tealiumStatus'})

t_email_completion = EmailOperator(
    task_id='email_etl_tealium_status_data',
    to=AirflowHelpers.get_dag_emails(dag.dag_id),
    retries=0,
    dag=dag,
    subject="AIRFLOW - COMPLETED: Tealium Customer Status Data Send",
    html_content="<h3>Tealium Customer Status Data Send to S3 has <font color=\"green\">completed.</font></h3><br>\
    {{ task_instance.xcom_pull(task_ids='run_etl_tealium_status_data', key='etl_results') }}")

t_tealium_status_data.set_downstream(t_email_completion)
