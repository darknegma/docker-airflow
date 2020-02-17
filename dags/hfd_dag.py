
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from sdc_etl_libs.airflow_helpers.AirflowHelpers import AirflowHelpers
from sdc_etl_libs.sdc_data_exchange.SDCDataExchange import SDCDataExchange


if Variable.get("environment") == "development":
    daily_charges_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    invalid_card_accounts_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    on_hold_accounts_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    communications_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    payment_report_submitted_funds_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    payment_report_submitted_funds_ca_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    customers_last_four_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    customers_using_portal_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    customers_without_hfd_account_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    customer_adjusted_term_lengths_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    customer_card_info_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    customer_disputes_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    jpm_data_tape_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    missed_and_manual_payments_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    aging_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    amortization_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    docd_and_activated_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    open_apps_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    payment_report_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    summary_and_detail_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    adjustments_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    arrangements_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    daily_account_dunning_metrics_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    jpm_datatape_monthly_mid_month_dpd_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    payments_due_billed_received_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    fee_maximums_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    failed_activations_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    daily_dunning_detail_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}

elif Variable.get("environment") == "production":
    daily_charges_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    invalid_card_accounts_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    on_hold_accounts_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    communications_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    payment_report_submitted_funds_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    payment_report_submitted_funds_ca_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    customers_last_four_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    customers_using_portal_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    customers_without_hfd_account_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    customer_adjusted_term_lengths_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    customer_card_info_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    customer_disputes_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    jpm_data_tape_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    missed_and_manual_payments_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    aging_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    amortization_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    docd_and_activated_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    open_apps_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    payment_report_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    summary_and_detail_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    adjustments_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    arrangements_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    daily_account_dunning_metrics_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    jpm_datatape_monthly_mid_month_dpd_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    payments_due_billed_received_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    fee_maximums_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    failed_activations_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    daily_dunning_detail_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}


def daily_charges_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/daily-charges", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Daily Charges data from S3 to Snowflake", result, **kwargs)


def invalid_card_accounts_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/invalid-card-accounts", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Invalid Card Accounts data from S3 to Snowflake", result, **kwargs)


def on_hold_accounts_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/on-hold-accounts", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "On Hold Accounts data from S3 to Snowflake", result, **kwargs)


def communications_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/communications", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Communications data from S3 to Snowflake", result, **kwargs)


def payment_report_submitted_funds_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/payment-report-submitted-funds", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Payment Report Submitted Funds data from S3 to Snowflake", result, **kwargs)


def payment_report_submitted_funds_ca_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/payment-report-submitted-funds-ca", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Payment Report Submitted Funds CA data from S3 to Snowflake", result, **kwargs)


def customers_last_four_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/customers-last-four", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Customers Last Four data from S3 to Snowflake", result, **kwargs)


def customers_using_portal_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/customers-using-portal", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Customers Using Portal data from S3 to Snowflake", result, **kwargs)


def customers_without_hfd_account_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/customers-without-hfd-account", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Customers Without HFD Account data from S3 to Snowflake", result, **kwargs)


def customer_adjusted_term_lengths_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/customer-adjusted-term-lengths", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Customer Adjusted Term Length data from S3 to Snowflake", result, **kwargs)


def customer_card_info_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/customer-card-info", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Customer Card Info data from S3 to Snowflake", result, **kwargs)


def customer_disputes_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/customer-disputes", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Customer Disputes data from S3 to Snowflake", result, **kwargs)


def jpm_data_tape_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/jpm-data-tape", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "JPM Data Tape data from S3 to Snowflake", result, **kwargs)


def missed_and_manual_payments_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/missed-and-manual-payments", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Missed and Manual Payments data from S3 to Snowflake", result, **kwargs)


def aging_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/aging", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Aging data from S3 to Snowflake", result, **kwargs)


def amortization_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/amortization", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Amortization data from S3 to Snowflake", result, **kwargs)


def docd_and_activated_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/docd-and-activated", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Docd and Activated data from S3 to Snowflake", result, **kwargs)


def open_apps_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/open-apps", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Open Apps data from S3 to Snowflake", result, **kwargs)


def payment_report_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/payment-report", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Payment Report data from S3 to Snowflake", result, **kwargs)


def summary_and_detail_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/summary-and-detail", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Summary and Detail data from S3 to Snowflake", result, **kwargs)


def adjustments_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/adjustments", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(

        "Adjustments data from S3 to Snowflake", result, **kwargs)


def arrangements_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/arrangements", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Arrangements data from S3 to Snowflake", result, **kwargs)


def daily_account_dunning_metrics_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/daily-account-dunning-metrics", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Daily Account Dunning Metrics data from S3 to Snowflake", result, **kwargs)


def jpm_datatape_monthly_mid_month_dpd_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/jpm-datatape-monthly-mid-month-dpd", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "JPM Datatape Monthly Mid-Month DPD data from S3 to Snowflake", result, **kwargs)


def payments_due_billed_received_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/payments-due-billed-received", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Payments Due Billed Received data from S3 to Snowflake", result, **kwargs)

def fee_maximums_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/fee-maximums", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Fee Maximums data from S3 to Snowflake", result, **kwargs)

def failed_activations_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/failed-activations", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Failed Activations data from S3 to Snowflake", result, **kwargs)

def daily_dunning_detail_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/daily-dunning-detail", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Daily Dunning Detail data from S3 to Snowflake", result, **kwargs)


default_args = {
    'owner': 'trevor.wnuk',
    'start_date': datetime(2019, 10, 15),
    'email': AirflowHelpers.get_dag_emails("data-eng"),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl-hfd',
    default_args=default_args,
    schedule_interval='0 15-18 * * *',
    dagrun_timeout=timedelta(hours=2),
    max_active_runs=1,
    catchup=False
)

run_daily_charges_to_db = PythonOperator(
    task_id='run_daily_charges_to_db',
    provide_context=True,
    python_callable=daily_charges_to_db,
    op_kwargs=daily_charges_to_db_args,
    dag=dag)

run_invalid_card_accounts_to_db = PythonOperator(
    task_id='run_invalid_card_accounts_to_db',
    provide_context=True,
    python_callable=invalid_card_accounts_to_db,
    op_kwargs=invalid_card_accounts_to_db_args,
    dag=dag)

run_on_hold_accounts_to_db = PythonOperator(
    task_id='run_on_hold_accounts_to_db',
    provide_context=True,
    python_callable=on_hold_accounts_to_db,
    op_kwargs=on_hold_accounts_to_db_args,
    dag=dag)

run_communications_to_db = PythonOperator(
    task_id='run_communications_to_db',
    provide_context=True,
    python_callable=communications_to_db,
    op_kwargs=communications_to_db_args,
    dag=dag)

run_payment_report_submitted_funds_to_db = PythonOperator(
    task_id='payment_report_submitted_funds_to_db',
    provide_context=True,
    python_callable=payment_report_submitted_funds_to_db,
    op_kwargs=payment_report_submitted_funds_to_db_args,
    dag=dag)

run_payment_report_submitted_funds_ca_to_db = PythonOperator(
    task_id='run_payment_report_submitted_funds_ca_to_db',
    provide_context=True,
    python_callable=payment_report_submitted_funds_ca_to_db,
    op_kwargs=payment_report_submitted_funds_ca_to_db_args,
    dag=dag)

run_customers_last_four_to_db = PythonOperator(
    task_id='run_customers_last_four_to_db',
    provide_context=True,
    python_callable=customers_last_four_to_db,
    op_kwargs=customers_last_four_to_db_args,
    dag=dag)

run_customers_using_portal_to_db = PythonOperator(
    task_id='run_customers_using_portal_to_db',
    provide_context=True,
    python_callable=customers_using_portal_to_db,
    op_kwargs=customers_using_portal_to_db_args,
    dag=dag)

run_customers_without_hfd_account_to_db = PythonOperator(
    task_id='run_customers_without_hfd_account_to_db',
    provide_context=True,
    python_callable=customers_without_hfd_account_to_db,
    op_kwargs=customers_without_hfd_account_to_db_args,
    dag=dag)

run_customer_adjusted_term_lengths_to_db = PythonOperator(
    task_id='run_customer_adjusted_term_lengths_to_db',
    provide_context=True,
    python_callable=customer_adjusted_term_lengths_to_db,
    op_kwargs=customer_adjusted_term_lengths_to_db_args,
    dag=dag)

run_customer_card_info_to_db = PythonOperator(
    task_id='run_customer_card_info_to_db',
    provide_context=True,
    python_callable=customer_card_info_to_db,
    op_kwargs=customer_card_info_to_db_args,
    dag=dag)

run_customer_disputes_to_db = PythonOperator(
    task_id='run_customer_disputes_to_db',
    provide_context=True,
    python_callable=customer_disputes_to_db,
    op_kwargs=customer_disputes_to_db_args,
    dag=dag)

run_jpm_data_tape = PythonOperator(
    task_id='run_jpm_data_tape',
    provide_context=True,
    python_callable=jpm_data_tape_to_db,
    op_kwargs=jpm_data_tape_to_db_args,
    dag=dag)

run_missed_and_manual_payments = PythonOperator(
    task_id='run_missed_and_manual_payments',
    provide_context=True,
    python_callable=missed_and_manual_payments_to_db,
    op_kwargs=missed_and_manual_payments_to_db_args,
    dag=dag)

run_aging = PythonOperator(
    task_id='run_aging',
    provide_context=True,
    python_callable=aging_to_db,
    op_kwargs=aging_to_db_args,
    dag=dag)

run_amortization = PythonOperator(
    task_id='run_amortization',
    provide_context=True,
    python_callable=amortization_to_db,
    op_kwargs=amortization_to_db_args,
    dag=dag)

run_docd_and_activated = PythonOperator(
    task_id='run_docd_and_activated',
    provide_context=True,
    python_callable=docd_and_activated_to_db,
    op_kwargs=docd_and_activated_to_db_args,
    dag=dag)

run_open_apps = PythonOperator(
    task_id='run_open_apps',
    provide_context=True,
    python_callable=open_apps_to_db,
    op_kwargs=open_apps_to_db_args,
    dag=dag)

run_payment_report = PythonOperator(
    task_id='run_payment_report',
    provide_context=True,
    python_callable=payment_report_to_db,
    op_kwargs=payment_report_to_db_args,
    dag=dag)

run_summary_and_detail = PythonOperator(
    task_id='run_summary_and_detail',
    provide_context=True,
    python_callable=summary_and_detail_to_db,
    op_kwargs=summary_and_detail_to_db_args,
    dag=dag)

run_adjustments = PythonOperator(
    task_id='run_adjustments',
    provide_context=True,
    python_callable=adjustments_to_db,
    op_kwargs=adjustments_to_db_args,
    dag=dag)

run_arrangements = PythonOperator(
    task_id='run_arrangements',
    provide_context=True,
    python_callable=arrangements_to_db,
    op_kwargs=arrangements_to_db_args,
    dag=dag)

run_daily_account_dunning_metrics = PythonOperator(
    task_id='run_daily_account_dunning_metrics',
    provide_context=True,
    python_callable=daily_account_dunning_metrics_to_db,
    op_kwargs=daily_account_dunning_metrics_to_db_args,
    dag=dag)

run_jpm_datatape_monthly_mid_month_dpd = PythonOperator(
    task_id='run_jpm_datatape_monthly_mid_month_dpd',
    provide_context=True,
    python_callable=jpm_datatape_monthly_mid_month_dpd_to_db,
    op_kwargs=jpm_datatape_monthly_mid_month_dpd_to_db_args,
    dag=dag)

run_payments_due_billed_received = PythonOperator(
    task_id='run_payments_due_billed_received',
    provide_context=True,
    python_callable=payments_due_billed_received_to_db,
    op_kwargs=payments_due_billed_received_to_db_args,
    dag=dag)

run_fee_maximums = PythonOperator(
    task_id='run_fee_maximums',
    provide_context=True,
    python_callable=fee_maximums_to_db,
    op_kwargs=fee_maximums_to_db_args,
    dag=dag)

run_failed_activations = PythonOperator(
    task_id='run_failed_activations',
    provide_context=True,
    python_callable=failed_activations_to_db,
    op_kwargs=failed_activations_to_db_args,
    dag=dag)

run_daily_dunning_detail = PythonOperator(
    task_id='run_daily_dunning_detail',
    provide_context=True,
    python_callable=daily_dunning_detail_to_db,
    op_kwargs=daily_dunning_detail_to_db_args,
    dag=dag)


generate_email = PythonOperator(
    task_id='generate_email',
    provide_context=True,
    op_kwargs={
            'etl_name_': "HFD",
            'tasks_': [task.task_id for task in dag.tasks],
            'environment_': Variable.get("environment")
        },
    python_callable=AirflowHelpers.generate_data_exchange_email,
    dag=dag)

send_email = EmailOperator(
    task_id='send_email',
    to=AirflowHelpers.get_dag_emails(dag.dag_id),
    retries=0,
    dag=dag,
    subject="{{ task_instance.xcom_pull(task_ids='generate_email', key='email_subject') }}",
    html_content="{{ task_instance.xcom_pull(task_ids='generate_email', key='email_body') }}"
)


(
    run_daily_charges_to_db,
    run_invalid_card_accounts_to_db,
    run_on_hold_accounts_to_db,
    run_communications_to_db,
    run_payment_report_submitted_funds_to_db,
    run_payment_report_submitted_funds_ca_to_db,
    run_customers_last_four_to_db,
    run_customers_using_portal_to_db,
    run_customers_without_hfd_account_to_db,
    run_customer_adjusted_term_lengths_to_db,
    run_customer_card_info_to_db,
    run_customer_disputes_to_db,
    run_jpm_data_tape,
    run_missed_and_manual_payments,
    run_aging,
    run_amortization,
    run_docd_and_activated,
    run_open_apps,
    run_payment_report,
    run_summary_and_detail,
    run_adjustments,
    run_arrangements,
    run_daily_account_dunning_metrics,
    run_jpm_datatape_monthly_mid_month_dpd,
    run_payments_due_billed_received,
    run_fee_maximums,
    run_failed_activations,
    run_daily_dunning_detail
) >> generate_email >> send_email
