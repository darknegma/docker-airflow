
import logging
from airflow_helpers.sdc_airflow_config import dag_emails
from sdc_etl_libs.sdc_data_exchange.SDCDataExchangeEnums import FileResultTypes


class AirflowHelpers:

    @staticmethod
    def combine_xcom_pulls_from_tasks(tasks_, key_='etl_results',
                                      return_html_=True, **kwargs):
        """
        Combines pulled xcom variables from a provided list of Airflow
        DAG task ids.

        :param tasks_: List of task ids.
        :param key_: Task key to pull from tasks.
        :param return_html_: If True, will format for use in HTML formatted
            e-mails.
        :param kwargs: Airflow kwargs.
        :return: Results as a string.
        """

        results = ""
        for task in tasks_:
            results = results + kwargs['ti'].xcom_pull(task_ids=task, key=key_) + \
                      ("<br>" if return_html_ else "\n")

        return results

    @staticmethod
    def push_xcom_variable(key_, value_, **kwargs):
        """
        Pushes an xcom variable to Airflow.

        :param key_: Task key..
        :param value_: Task value
        :param kwargs: Airflow kwargs.
        :return: String message of what value was pushed to what key.
        """

        kwargs['ti'].xcom_push(key=key_, value=value_)

        return f"{value_} pushed to key {key_}"

    def get_dag_emails(dag_id_):
        """
        Given a Airflow DAG ID (ex. 'etl-hfd-reports'), returns a list of
        e-mails that need to be notified when DAG completes. If the
        DAG ID does not exist in the config file, returns the default
        Data Engineering e-mail.

        :param dag_id_: DAG id.
        :return: List of e-mails.
        """

        default_email = dag_emails["data-eng"]

        if dag_id_ not in dag_emails.keys():
            logging.warning(f"E-mail list not set for dag {dag_id_}. "
                         f"Using default data-eng email.")

        return dag_emails.get(dag_id_, default_email)

    @staticmethod
    def process_etl_results_log(header_, etl_results_log_, **kwargs):
        """
        Formats ETL log results from data exchange.
        :param header_: Header for ETL results.
        :param etl_results_log_: Dictionary produced from SDCDataExchage
            exchange_data() with log results.
        :return: Results as string.
        """

        msg = f"<h3>{header_}:</h3>"
        for tag, logs in etl_results_log_.items():
            logs.sort(reverse=True)
            msg += f"<h4>{tag}</h4>"
            for log in logs:
                if "error" in log.lower():
                    result = f"<font color=\"red\">{log}</font>"
                else:
                    result = log
                msg = msg + result + "<br>"

        AirflowHelpers.push_xcom_variable('etl_results', msg, **kwargs)

        return msg

    @staticmethod
    def generate_data_exchange_email(etl_name_=None, tasks_=None,
                                     environment_=None, **kwargs):
        """
        Generates Airflow completion e-mail for Data Exchange. Generates header, body and subject
        based on the etl_name_ and tasks_ passed in as op_kwargs in PythonOperator.
        :param etl_name_: ETL name (will appear in Subject / Body of e-mail.
            Defaults to DAG name via "dag.dag_id"
        :param tasks_: List of tasks to include in the e-mail.
            Defaults to all tasks associated with DAG via "[task.task_id for task in dag.tasks]"
        :param environment_: If "development", flag is added to e-mail subject to note
            data exchange affected development environment.
        :param kwargs: Airflow kwargs
        :return: None, but, pushes the following two xcom variables for use in DAG script:
            - email_subject
            - email_body
        Example use in DAG file:
            # Pass in tasks to include in e-mail generation and name of ETL
            generate_email = PythonOperator(
                task_id='generate_email',
                provide_context=True,
                op_kwargs={
                        'etl_name_': "Logic Plum",
                        'tasks_': ["logic_plum_scan_show_rates_s3_to_sftp",
                                  "logic_plum_scan_show_rates_s3_to_db"]
                    },
                python_callable=SDCDataExchange.generate_airflow_completion_email,
                dag=dag)
            # Reference the email_subject and email_body xcoms pushed from generate_email task
            send_email = EmailOperator(
                task_id='send_email',
                to=AirflowHelpers.get_dag_emails(dag.dag_id),
                retries=0,
                dag=dag,
                subject="{{ task_instance.xcom_pull(task_ids='generate_email', key='email_subject') }}",
                html_content="{{ task_instance.xcom_pull(task_ids='generate_email', key='email_body') }}"
            )
            # Make sure generate_email task is upstream of send_email in graph
            (
                logic_plum_scan_show_rates_s3_to_sftp,
                logic_plum_scan_show_rates_s3_to_db
            ) >> generate_email >> send_email
        """

        if not etl_name_:
            etl_name_ = dag.dag_id

        if not tasks_:
            [task.task_id for task in dag.tasks]

        body = f"<h3>{etl_name_} ETL has <font color=\"green\">completed.</font></h3>"

        body = body + AirflowHelpers.combine_xcom_pulls_from_tasks(tasks_, **kwargs)

        errors = body.count(f"{FileResultTypes.error.value}: ")
        successes = body.count(f"{FileResultTypes.success.value}: ")
        skipped = body.count(f"{FileResultTypes.empty.value}: ")
        dev_flag = '**DEV** ' if environment_ == 'development' else ''

        subject = f"{dev_flag}{etl_name_}: AIRFLOW Completed " + \
                  ("w/ Errors " if errors > 0 else "") + \
                  f"({errors} error(s), {successes} success(es), {skipped} skipped)"

        AirflowHelpers.push_xcom_variable('email_body', body, **kwargs)
        AirflowHelpers.push_xcom_variable('email_subject', subject, **kwargs)

        return body, subject, errors, successes, skipped
