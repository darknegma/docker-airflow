
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import PythonOperator
from airflow.operators.email_operator import EmailOperator
from scripts.horizon_web_data_s3 import HorizonWebData
from sdc_etl_libs.airflow_helpers.AirflowHelpers import AirflowHelpers


default_args = {
    'owner': 'trevorwnuk',
    'start_date': datetime(2019, 5, 5),
    'email': AirflowHelpers.get_dag_emails("data-eng"),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'etl-horizon-web-data',
    default_args=default_args,
    schedule_interval='00 07 * * *',
    dagrun_timeout=timedelta(hours=2))

t_horizon_web_data = PythonOperator(
    task_id='run_etl_horizon_web_data',
    provide_context=True,
    python_callable=HorizonWebData.horizon_snowflake_to_s3,
    op_kwargs={'seasoning_': 90},
    dag=dag)

t_email_completion = EmailOperator(
    task_id='email_etl_horizon_web_data',
    to=AirflowHelpers.get_dag_emails(dag.dag_id),
    retries=0,
    dag=dag,
    subject="AIRFLOW - COMPLETED: Horizon Next Web Data Send",
    html_content="<h3>The Horizon Next Web Data script has completed.</h3>")

t_horizon_web_data.set_downstream(t_email_completion)
