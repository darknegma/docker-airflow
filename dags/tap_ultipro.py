
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta
from sdc_etl_libs.airflow_helpers.AirflowHelpers import AirflowHelpers

default_args = {
    'owner': 'fabio.lissi',
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
ULTIPRO_USER = "{{ var.value.ultipro_user }}"
ULTIPRO_PWD = "{{ var.value.ultipro_password }}"


dag = DAG(
    'tap-ultipro', 
    default_args=default_args,
    schedule_interval='00 07 * * *',
    dagrun_timeout=timedelta(hours=2)
    )

t_run_ultipro = BashOperator(
    task_id='run_ultipro_tap',
    bash_command=PYTHON_PATH + 'python ' + TAP_PATH + 'tap-ultipro/tap_ultipro/__init__.py | ' + TARGET_STITCH + ' -c ' + TAP_PATH + 'tap-ultipro/tap_ultipro/persist.json',
    retries=3,
    dag=dag,
    env={'ultipro_user': ULTIPRO_USER,
        'ultipro_password': ULTIPRO_PWD})

t_email_completion = EmailOperator(
    task_id='email_ultipro_complete',
    to=AirflowHelpers.get_dag_emails(dag.dag_id),
    retries=0,
    dag=dag,
    subject="AIRFLOW - COMPLETED: Ultipro Tap Complete",
    html_content="<h3>The Ultipro Tap has completed.</h3>")


t_run_ultipro >> t_email_completion
