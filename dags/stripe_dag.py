
from datetime import datetime, timedelta
from enum import Enum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from scripts.Stripe.stripe_api import Stripe
from sdc_etl_libs.airflow_helpers.AirflowHelpers import AirflowHelpers


class DBRoles(Enum):
    NON_PII_ROLE = '"AIRFLOW_SERVICE_ROLE"'
    PII_ROLE = '"AIRFLOW_WAREHOUSE.RAW.STRIPE.pii.service.role"'


class StripeAPISecrets(Enum):
    STRIPE_USA = "de/stripe-api-usa"
    STRIPE_CAN = "de/stripe-api-can"
    STRIPE_AUS = "de/stripe-api-aus"
    STRIPE_NZL = "stripe-nzl/api"
    STRIPE_GBR = "stripe-gbr/api"


default_args = {
    'owner': 'trevor.wnuk',
    'start_date': datetime(2019, 7, 14),
    'email': AirflowHelpers.get_dag_emails("data-eng"),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl-stripe-api',
    default_args=default_args,
    schedule_interval='00 10 * * *',
    dagrun_timeout=timedelta(hours=2)
)

stripe_get_payment_method_details_usa = PythonOperator(
    task_id='stripe_get_payment_method_details_usa',
    provide_context=True,
    python_callable=Stripe.get_payment_method_details,
    op_kwargs={'api_secrets_': StripeAPISecrets.STRIPE_USA.value,
               'table_name_': 'payment_method_details_usa_pii',
               'db_role_': DBRoles.PII_ROLE.value},
    dag=dag)

stripe_get_payment_method_details_can = PythonOperator(
    task_id='stripe_get_payment_method_details_can',
    provide_context=True,
    python_callable=Stripe.get_payment_method_details,
    op_kwargs={'api_secrets_': StripeAPISecrets.STRIPE_CAN.value,
               'table_name_': 'payment_method_details_can_pii',
               'db_role_': DBRoles.PII_ROLE.value},
    dag=dag)

stripe_get_payment_method_details_aus = PythonOperator(
    task_id='stripe_get_payment_method_details_aus',
    provide_context=True,
    python_callable=Stripe.get_payment_method_details,
    op_kwargs={'api_secrets_': StripeAPISecrets.STRIPE_AUS.value,
               'table_name_': 'payment_method_details_aus_pii',
               'db_role_': DBRoles.PII_ROLE.value},
    dag=dag)

stripe_get_payment_method_details_nzl = PythonOperator(
    task_id='stripe_get_payment_method_details_nzl',
    provide_context=True,
    python_callable=Stripe.get_payment_method_details,
    op_kwargs={'api_secrets_': StripeAPISecrets.STRIPE_NZL.value,
               'table_name_': 'payment_method_details_nzl_pii',
               'db_role_': DBRoles.PII_ROLE.value},
    dag=dag)

stripe_get_payment_method_details_gbr = PythonOperator(
    task_id='stripe_get_payment_method_details_gbr',
    provide_context=True,
    python_callable=Stripe.get_payment_method_details,
    op_kwargs={'api_secrets_': StripeAPISecrets.STRIPE_GBR.value,
               'table_name_': 'payment_method_details_gbr_pii',
               'db_role_': DBRoles.PII_ROLE.value},
    dag=dag)


email_completion = EmailOperator(
    task_id='email_complete_stripe',
    to=AirflowHelpers.get_dag_emails(dag.dag_id),
    retries=0,
    dag=dag,
    subject="AIRFLOW - COMPLETED: Stripe",
    html_content="<h3>Stripe data has been updated</h3><br>\
    <b>Tables updated:</b><br>\
    {{ task_instance.xcom_pull(""task_ids='stripe_get_payment_method_details_usa', key='etl_results') }}\
    {{ task_instance.xcom_pull(""task_ids='stripe_get_payment_method_details_can', key='etl_results') }}\
    {{ task_instance.xcom_pull(""task_ids='stripe_get_payment_method_details_nzl', key='etl_results') }}\
    {{ task_instance.xcom_pull(""task_ids='stripe_get_payment_method_details_gbr', key='etl_results') }}\
    {{ task_instance.xcom_pull(""task_ids='stripe_get_payment_method_details_aus', key='etl_results') }}"
)

(
    stripe_get_payment_method_details_usa,
    stripe_get_payment_method_details_can,
    stripe_get_payment_method_details_aus,
    stripe_get_payment_method_details_nzl,
    stripe_get_payment_method_details_gbr
) >> email_completion
