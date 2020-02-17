
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import PythonOperator
from airflow.operators.email_operator import EmailOperator
from scripts.finance_smilepay_data_db import FinanceSmilepay
from sdc_etl_libs.airflow_helpers.AirflowHelpers import AirflowHelpers


default_args = {
    'owner': 'trevor.wnuk',
    'start_date': datetime(2019, 6, 19),
    'email': AirflowHelpers.get_dag_emails("data-eng"),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}


dag = DAG(
    'finance-xf-smilepay-data',
    default_args=default_args,
    schedule_interval='00 14 * * *',
    dagrun_timeout=timedelta(hours=2))

t_xf_smilepay_interest_data = PythonOperator(
    task_id='run_update_smilepay_interest_data',
    provide_context=True,
    python_callable=FinanceSmilepay.smilepay_interest_schedules,
    dag=dag)

t_email_completion = EmailOperator(
    task_id='email_finance_xf_smilepay_data',
    to=AirflowHelpers.get_dag_emails(dag.dag_id),
    retries=0,
    dag=dag,
    subject="AIRFLOW - COMPLETED: Finance Smilepay Data Transformations",
    html_content="<h3>Finance Smilepay Data Transformations have "
                 "<font color=\"green\">completed.</font></h3><br>"
                 "{{ task_instance.xcom_pull("
                 "task_ids='run_update_smilepay_interest_data', "
                 "key='etl_results') }}")

t_xf_smilepay_interest_data.set_downstream(t_email_completion)
