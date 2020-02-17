import sys, os
import pytest
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../../")
from sdc_etl_libs.airflow_helpers.AirflowHelpers import AirflowHelpers

def test_email_with_errors(mocker):

    mocker.patch('sdc_etl_libs.airflow_helpers.AirflowHelpers.AirflowHelpers.combine_xcom_pulls_from_tasks',
                 return_value="""<h3>iCIMS ETL has <font color=\"green\">completed.</font><br>
                                <b>iCIMS Persons PII data from SFTP to Snowflake:</b><br>
                                SUCCESS: Loaded export.Person09.10.2019.csv to DB. Inserted 626 record(s).<br>
                                ERROR: Loading export.Person07.23.2019.csv to DataFrame failed.<br>
                                ERROR: Loading export.Person07.22.2019.csv to DataFrame failed.<br>
                                ERROR: Loading export.Person07.20.2019.csv to DataFrame failed.<br>
                                ERROR: Loading export.Person07.19.2019.csv to DataFrame failed.<br>
                                ERROR: Loading export.Person07.18.2019.csv to DataFrame failed.<br>
                                ERROR: Loading export.Person07.17.2019.csv to DataFrame failed.<br>
                                ERROR: Loading export.Person07.16.2019.csv to DataFrame failed.<br>
                                <br>
                                <b>iCIMS Job PII data from SFTP to Snowflake:</b><br>
                                ERROR: Loading export_Job.09.10.2019.csv to DataFrame failed.<br>
                                ERROR: Loading export_Job.09.09.2019.csv to DataFrame failed.<br>
                                ERROR: Loading export_Job.09.08.2019.csv to DataFrame failed.<br>
                                ERROR: Loading export_Job.09.07.2019.csv to DataFrame failed.<br>
                                ERROR: Loading export_Job.09.06.2019.csv to DataFrame failed.<br>
                                ERROR: Loading export_Job.09.05.2019.csv to DataFrame failed.<br>
                                ERROR: Loading export_Job.09.04.2019.csv to DataFrame failed.<br>
                                ERROR: Loading export_Job.09.03.2019.csv to DataFrame failed.<br>
                                ERROR: Loading export_Job.09.02.2019.csv to DataFrame failed.<br>
                                <br>
                                <b>iCIMS Candidate Workflow PII data from SFTP to Snowflake:</b><br>
                                SUCCESS: Loaded export_Candidate_Workflow.09.10.2019.csv to DB. Inserted 2,782 record(s).<br>
                                ERROR: Loading export_Candidate_Workflow.07.25.2019.csv to DataFrame failed.<br>
                                ERROR: Loading export_Candidate_Workflow.07.24.2019.csv to DataFrame failed.<br>
                                ERROR: Loading export_Candidate_Workflow.07.23.2019.csv to DataFrame failed.<br>
                                ERROR: Loading export_Candidate_Workflow.07.22.2019.csv to DataFrame failed.<br>
                                """)

    mocker.patch('sdc_etl_libs.airflow_helpers.AirflowHelpers.AirflowHelpers.push_xcom_variable', return_value=None)

    body, subject, errors, successes = \
        AirflowHelpers.generate_data_exchange_email(etl_name_="iCIMS", tasks_=["persons", "jobs", "workflow"])

    pytest.assume(errors == 20)
    pytest.assume(successes == 2)
    pytest.assume("Completed w/ Errors" in subject)
    pytest.assume(body.count("ERROR: ") == 20)
    pytest.assume(body.count("SUCCESS: ") == 2)

def test_email_without_errors(mocker):

    mocker.patch('sdc_etl_libs.airflow_helpers.AirflowHelpers.AirflowHelpers.combine_xcom_pulls_from_tasks',
                 return_value="""<h3>iCIMS ETL has <font color=\"green\">completed.</font><br>
                                <b>iCIMS Persons PII data from SFTP to Snowflake:</b><br>
                                SUCCESS: Loaded export.Person09.10.2019.csv to DB. Inserted 626 record(s).<br>
                                <br>
                                <b>iCIMS Job PII data from SFTP to Snowflake:</b><br>
                                SUCCESS: Loaded export.Job09.10.2019.csv to DB. Inserted 626 record(s).<br>
                                <br>
                                <b>iCIMS Candidate Workflow PII data from SFTP to Snowflake:</b><br>
                                SUCCESS: Loaded export_Candidate_Workflow.09.10.2019.csv to DB. Inserted 2,782 record(s).<br>
                                """)

    mocker.patch('sdc_etl_libs.airflow_helpers.AirflowHelpers.AirflowHelpers.push_xcom_variable', return_value=None)

    body, subject, errors, successes = \
        AirflowHelpers.generate_data_exchange_email(etl_name_="iCIMS", tasks_=["persons", "jobs", "workflow"])

    pytest.assume(errors == 0)
    pytest.assume(successes == 3)
    pytest.assume("Completed w/ Errors" not in subject)
    pytest.assume(body.count("ERROR: ") == 0)
    pytest.assume(body.count("SUCCESS: ") == 3)
