import sys
import os
import json
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../../")
from sdc_etl_libs.sdc_data_schema.SDCDataSchema import SDCDataSchema
import pytest

test_schema_1_no_issues = json.loads(open(
    os.path.dirname(os.path.abspath(__file__)) + "/test_schema_1_no_issues.json").read())
test_schema_2_wrong_data_type = json.loads(open(
    os.path.dirname(os.path.abspath(__file__)) + "/test_schema_2_wrong_data_type.json").read())
test_schema_3_not_tagged_properly = json.loads(open(
    os.path.dirname(os.path.abspath(__file__)) + "/test_schema_3_not_tagged_properly.json").read())
test_schema_4_optional_item_missing = json.loads(open(
    os.path.dirname(os.path.abspath(__file__)) + "/test_schema_4_optional_item_missing.json").read())
test_schema_5_multiple_tags = json.loads(open(
    os.path.dirname(os.path.abspath(__file__)) + "/test_schema_5_multiple_tags.json").read())


def test_get_field_names_for_file():
    """
    Verify that if column names are needed from the data schema (ex. file being
    processed does not have headers) that all field names that are not added
    columns are returned as a list and in order
    """

    # All fields (including added columns)
    assert len(test_schema_1_no_issues["fields"]) == 17

    # Verify type returned and fields that should be returned
    result = SDCDataSchema.get_field_names_for_file(test_schema_1_no_issues)
    assert type(result) == list
    assert len(result) == 15

    # Verify order of list
    assert result[0] == "ID1"
    assert result[5] == "ID6"

def test_parse_endpoint_data():

    result = SDCDataSchema.parse_endpoint_data(test_schema_1_no_issues, "SDC_sink_0")

    # Result should be dict
    assert type(result) == dict
    # There should at least be 1 key
    assert len(result) >= 1
    # Assert expected values
    expected_result = {'type': 's3', 'tag': 'SDC_sink_0', 'endpoint_type': 'sink',
     'bucket': 'sdc-shipment-tracking', 'prefix': 'tnt/', 'region': 'us-east-2',
     'file_info': {'type': 'csv', 'delimiter': '|', 'file_regex': '.*ECS',
                   'headers': False, 'no_schema_validation': None,
                   'specific_column_order': None, 'decode': None},
     'credentials': None}
    assert result == expected_result

def test_parse_endpoint_data_failure_when_tag_does_not_exist():
    """
    Need to raise an exception if the tag does not exist.
    """

    with pytest.raises(Exception) as excep_info:
        SDCDataSchema.parse_endpoint_data(test_schema_1_no_issues, "NOT_A_TAG")
    assert "No records round for tag:" in str(excep_info.value)

def test_parse_endpoint_data_failure_when_missing_tags():
    """
    Need to raise an exception if the tag does not exist.
    """

    with pytest.raises(Exception):
        SDCDataSchema.parse_endpoint_data(test_schema_3_not_tagged_properly, "SDC_sink_0")

def test_parse_endpoint_data_failure_when_multiple_tags():
    """
    Need to raise an exception if a tag is used more than once.
    """

    with pytest.raises(Exception) as excep_info:
        SDCDataSchema.parse_endpoint_data(test_schema_5_multiple_tags, "SDC_sink_0")
    assert "Failed parsing schema data. 2 records found for tag: SDC_sink_0" in str(excep_info.value)

def test_parse_endpoint_data_failure_when_wrong_data_type():
    """
    If a data type is not the correct type as per the Enum, we expect an exception to
    be raised.
    test_schema_2_wrong_data_type for source/s3 has the "file_info":"headers" item set
        with a non-boolean value
    """

    with pytest.raises(Exception) as excep_info:
        SDCDataSchema.parse_endpoint_data(test_schema_2_wrong_data_type, "main_source")
    assert "Schema failed validation ERROR: Schema validation for FILE_INFO failed due to" in str(excep_info.value)

def test_parse_endpoint_data_missing_optioanl_item_added():
    """
    If an optional item is missing in the schema, we expect that, during parsing, it is added
    to the schema with a value of None.
    test_schema_4_optional_item_missing for sink/s3 has the optional "prefix" item removed.
    """

    result = SDCDataSchema.parse_endpoint_data(test_schema_4_optional_item_missing, "SDC_sink_0")

    assert result["prefix"] is None