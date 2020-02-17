from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'fabio.lissi',
    'start_date': datetime(2019, 2, 7),
    'email': ['sdcde@smiledirectclub.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

PYTHON_PATH = "{{ var.value.PYTHON_PATH }}"
SCRIPT_PATH = "{{ var.value.SCRIPT_PATH }}"
SNOWFLAKE_USER = "{{ var.value.snowflake_user }}"
SNOWFLAKE_PWD = "{{ var.value.snowflake_password }}"
SNOWFLAKE_ACCOUNT = "{{ var.value.snowflake_account }}"
AWS_KEY_ID =  "{{ var.value.aws_authorization }}"
AWS_SECRET_KEY = "{{ var.value.aws_secret_key }}"

dag = DAG(
    'tap-horizon-daily-clearance', default_args=default_args, schedule_interval='00 05 * * FRI',
    dagrun_timeout=timedelta(hours=2))

t_run_horizon_daily_clearance = BashOperator(
    task_id='run_horizon_daily_clearance',
    bash_command=PYTHON_PATH + 'python ' + SCRIPT_PATH + 'horizon_daily_clearance.py',
    retries=3,
    dag=dag,
    env={
        'SNOWFLAKE_USER': SNOWFLAKE_USER,
        'SNOWFLAKE_PWD': SNOWFLAKE_PWD,
        'SNOWFLAKE_ACCOUNT': SNOWFLAKE_ACCOUNT,
        'AWS_KEY_ID': AWS_KEY_ID,
        'AWS_SECRET_KEY': AWS_SECRET_KEY})

t_email_completion = EmailOperator(
    task_id='email_horizon_daily_clearance_complete',
    to=["sdcde@smiledirectclub.com"],
    retries=0,
    dag=dag,
    subject="Horizon Daily Clearance Complete",
    html_content="<h3>The Horizon Daily Clearance has been completed.</h3>")
t_email_completion.set_upstream(t_run_horizon_daily_clearance)