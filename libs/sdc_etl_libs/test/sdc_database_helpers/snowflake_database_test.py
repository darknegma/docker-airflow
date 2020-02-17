import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../../")
from sdc_etl_libs.database_helpers.SnowflakeDatabase import SnowflakeDatabase
import pytest


def test_clean_column_names():

    # All characters to uppercase
    assert SnowflakeDatabase.clean_column_name("total_sales") == "TOTAL_SALES"
    # Spaces to underscore
    assert SnowflakeDatabase.clean_column_name("TOTAL SALES") == "TOTAL_SALES"
    # Trailing space trimmed
    assert SnowflakeDatabase.clean_column_name("TOTAL_SALES ") == "TOTAL_SALES"
    # Multiple underscores to single undercore
    assert SnowflakeDatabase.clean_column_name("TOTAL____SALES__EOM ") == "TOTAL_SALES_EOM"
    # Octothorp to NUM
    assert SnowflakeDatabase.clean_column_name("# Of Stores") == "NUM_OF_STORES"
    # Non-alphanumeric to underscore
    assert SnowflakeDatabase.clean_column_name("Employee: Last Name, First Name") == "EMPLOYEE_LAST_NAME_FIRST_NAME"

    # Extremely messy column names
    assert SnowflakeDatabase.clean_column_name("Recruiting Workflow Profile (Person Full Name: First, Last Label)") \
           == "RECRUITING_WORKFLOW_PROFILE_PERSON_FULL_NAME_FIRST_LAST_LABEL"
    assert SnowflakeDatabase.clean_column_name("_____1 # Email -:     Payment Failed________    ") \
           == "_1_NUM_EMAIL_PAYMENT_FAILED"
