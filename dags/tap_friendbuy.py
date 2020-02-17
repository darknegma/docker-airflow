
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from sdc_etl_libs.airflow_helpers.AirflowHelpers import AirflowHelpers


default_args = {
    'owner': 'trevor.wnuk',
    'start_date': datetime(2019, 5, 12),
    'email': AirflowHelpers.get_dag_emails("data-eng"),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

PYTHON_PATH = "{{ var.value.PYTHON_PATH }}"
TAP_PATH = "{{ var.value.TAP_PATH }}"
TARGET_STITCH = "{{ var.value.TARGET_STITCH }}"
ACCESS_KEY = "{{ var.value.friendbuy_access_key }}"
SECRET_KEY ="{{ var.value.friendbuy_secret_key }}"


dag = DAG(
    'tap-friendbuy', 
    default_args=default_args,
    schedule_interval='35 07 * * *',
    dagrun_timeout=timedelta(hours=2)
    )

t_run_friendbuy = BashOperator(
    task_id='run_friendbuy_tap',
    bash_command=PYTHON_PATH + 'python ' + TAP_PATH + 'tap-friendbuy/tap_friendbuy/__init__.py | ' + TARGET_STITCH + ' -c ' + TAP_PATH + 'tap-friendbuy/tap_friendbuy/persist.json',
    retries=3,
    dag=dag,
    env={'friendbuy_access_key': ACCESS_KEY,
        'friendbuy_secret_key': SECRET_KEY})

t_email_completion = EmailOperator(
    task_id='email_friendbuy_complete',
    to=AirflowHelpers.get_dag_emails(dag.dag_id),
    retries=0,
    dag=dag,
    subject="AIRFLOW - COMPLETED: Friendbuy Tap Complete",
    html_content="<h3>The Friendbuy Tap has <font color=\"green\">completed.</h3>")


t_run_friendbuy >> t_email_completion
