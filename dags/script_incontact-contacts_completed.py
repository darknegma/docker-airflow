from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'jeffhorn',
    'start_date': datetime(2019, 4, 9),
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
INCONTACT_AUTHORIZATION = "{{ var.value.incontact_authorization }}"
INCONTACT_USERNAME = "{{ var.value.incontact_username }}"
INCONTACT_PASSWORD = "{{ var.value.incontact_password }}"

dag = DAG(
    'script-incontact-contacts-created_v1', default_args=default_args, schedule_interval='05 02 * * *',
    dagrun_timeout=timedelta(hours=2))

t_run_incontact_contacts_created = BashOperator(
    task_id='run_incontact-contacts_created',
    bash_command=PYTHON_PATH + 'python ' + SCRIPT_PATH + 'incontact-contacts_completed.py',
    retries=3,
    dag=dag,
    env={
        'SNOWFLAKE_USER': SNOWFLAKE_USER,
        'SNOWFLAKE_PWD': SNOWFLAKE_PWD,
        'SNOWFLAKE_ACCOUNT': SNOWFLAKE_ACCOUNT,
        'INCONTACT_AUTHORIZATION': INCONTACT_AUTHORIZATION,
        'INCONTACT_USERNAME': INCONTACT_USERNAME,
        'INCONTACT_PASSWORD': INCONTACT_PASSWORD}
)

t_email_completion = EmailOperator(
    task_id='email_incontact-contacts_created_complete',
    to=["sdcde@smiledirectclub.com"],
    retries=0,
    dag=dag,
    subject="Incontact Contacts Created Import Complete",
    html_content="<h3>The Incontact Contacts Created Import  script has been completed.</h3>")
t_email_completion.set_upstream(t_run_incontact_contacts_created)
