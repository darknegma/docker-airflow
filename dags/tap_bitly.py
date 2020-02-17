
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from sdc_etl_libs.airflow_helpers.AirflowHelpers import AirflowHelpers


default_args = {
    'owner': 'fabio.lissi',
    'start_date': datetime(2018, 12, 17),
    'email': AirflowHelpers.get_dag_emails("data-eng"),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

PYTHON_PATH = "{{ var.value.PYTHON_PATH }}"
TAP_PATH = "{{ var.value.TAP_PATH }}"
TARGET_STITCH = "{{ var.value.TARGET_STITCH }}"
BITLY_AUTHORIZATION = "{{ var.value.bitly_authorization_key }}"
SNOWFLAKE_USER = "{{ var.value.snowflake_user }}"
SNOWFLAKE_PWD = "{{ var.value.snowflake_password }}"
SNOWFLAKE_ACCOUNT = "{{ var.value.snowflake_account }}"

dag = DAG(
    'tap-bitly', default_args=default_args, schedule_interval='05 19 * * *',
    dagrun_timeout=timedelta(hours=2))

t_run_bitly = BashOperator(
    task_id='run_bitly_tap',
    bash_command=PYTHON_PATH + 'python ' + TAP_PATH + 'tap-bitly/tap_bitly/__init__.py | ' + TARGET_STITCH + ' -c ' + TAP_PATH + 'tap-bitly/tap_bitly/persist.json',
    retries=3,
    dag=dag,
    env={'BITLY_AUTHORIZATION': BITLY_AUTHORIZATION,
        'SNOWFLAKE_USER': SNOWFLAKE_USER,
        'SNOWFLAKE_PWD': SNOWFLAKE_PWD,
        'SNOWFLAKE_ACCOUNT': SNOWFLAKE_ACCOUNT})

t_email_completion = EmailOperator(
    task_id='email_bitly_complete',
    to=AirflowHelpers.get_dag_emails(dag.dag_id),
    retries=0,
    dag=dag,
    subject="AIRFLOW - COMPLETED: Bitly Tap Complete",
    html_content="<h3>The Bitly tap has been completed.</h3>")
t_email_completion.set_upstream(t_run_bitly)