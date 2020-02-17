import sys
import math
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../../")
from sdc_etl_libs.sdc_dataframe.Dataframe import *
import pandas as pd
import numpy as np
import json
import pytest

def test_schema_generation():
    test_schema = """
        {
            "namespace": "Dataframe",
            "type": "object",
            "name": "I am a test",
            "fields": [
                {"name":"Id1","type":{"type":"string"}},
                {"name":"Id2","type":{"type":"int"}},
                {"name":"Id3","type":{"type":"boolean"}},
                {"name":"Id4","type":{"type":"long"}},
                {"name":"Id5","type":{"type":"float"}},
                {"name":"Id6","type":{"type":"double"}},
                {"name":"Id7","type":{"type":"string", "logical_type":"datetime"}}
            ]
        }
        """

    expected_result = [{"ID1": np.object_},{"ID2":pd.Int32Dtype()},{"ID3":np.bool_},{"ID4":pd.Int64Dtype()},{"ID5":np.float32},{"ID6":np.float64},{"ID7": np.datetime64}]
    df = Dataframe(SDCDFTypes.PANDAS, test_schema)
    assert len(df.df_schema) == len(expected_result)
    for i in range(len(expected_result)):
        assert(df.df_schema[i] == expected_result[i])

def test_load_data(mocker):
    mocker.patch('pandas.Timestamp.utcnow', return_value=pd.Timestamp('2019-07-09 22:32:33.135693+00:00').tz_localize(None))
    test_schema = """
        {
            "namespace": "Dataframe",
            "type": "object",
            "name": "I am a test",
            "fields": [
                {"name":"Id1","type":{"type":"string"}},
                {"name":"Id2","type":{"type":"int"}},
                {"name":"Id3","type":{"type":"boolean"}},
                {"name":"Id4","type":{"type":"long"}},
                {"name":"Id5","type":{"type":"float"}},
                {"name":"Id6","type":{"type":"double"}},
                {"name":"Id7","type":{"type":"string", "logical_type":"datetime", "add_column":true}}
            ]
        }
        """
    test_data = """
        {
                "Id1":"foo",
                "Id2":10,
                "Id3": false,
                "Id4":223372036854777807,
                "Id5":5.555,
                "Id6": 2000.55,
                "Id7": "2019-07-09 22:32:33.135693+00:00"
        }
        """
    df = Dataframe(SDCDFTypes.PANDAS, test_schema)
    df.load_data([json.loads(test_data)])
    assert df.df.iloc[0]["ID1"] == "foo"
    assert df.df.iloc[0]["ID2"] == 10
    assert df.df.iloc[0]["ID3"] == False
    assert df.df.iloc[0]["ID4"] == 223372036854777807
    assert math.isclose(df.df.iloc[0]["ID5"], 5.555, abs_tol=0.001)
    assert math.isclose(df.df.iloc[0]["ID6"], 2000.55, abs_tol=0.01)
    assert str(df.df.iloc[0]["ID7"]) == '2019-07-09 22:32:33.135693'

    assert df.df["ID1"].dtype == SDCDFPandasTypes.string.value
    assert df.df["ID2"].dtype == SDCDFPandasTypes.int.value
    assert df.df["ID3"].dtype == SDCDFPandasTypes.boolean.value
    assert df.df["ID4"].dtype == SDCDFPandasTypes.long.value
    assert df.df["ID5"].dtype == SDCDFPandasTypes.float.value
    assert df.df["ID6"].dtype == SDCDFPandasTypes.double.value
    assert df.df["ID7"].dtype == SDCDFPandasTypes.datetime.value or df.df["ID7"].dtype == np.dtype('>M8[ns]') or df.df["ID7"].dtype == np.dtype('<M8[ns]')


def test_bad_data(mocker):
    mocker.patch('pandas.Timestamp.utcnow', return_value=pd.Timestamp('2019-07-09 22:32:33.135693+00:00').tz_localize(None))
    test_schema = """
        {
            "namespace": "Dataframe",
            "type": "object",
            "name": "I am a test",
            "fields": [
                {"name":"Id1","type":{"type":"string"}},
                {"name":"Id2","type":{"type":"int"}},
                {"name":"Id3","type":{"type":"boolean"}},
                {"name":"Id4","type":{"type":"long"}},
                {"name":"Id5","type":{"type":"float"}},
                {"name":"Id6","type":{"type":"double"}},
                {"name":"Id7","type":{"type":"string", "logical_type":"datetime"}}
            ]
        }
        """
    test_data = """
        {
                "Id1":"foo",
                "Id2":"",
                "Id3": false,
                "Id4":223372036854777807,
                "Id5":5.555,
                "Id6": 2000.55,
                "Id7": ""
        }
        """
    df = Dataframe(SDCDFTypes.PANDAS, test_schema)
    df.load_data([json.loads(test_data)])
    assert df.df.iloc[0]["ID1"] == "foo"
    assert type(df.df.iloc[0]["ID2"]) == type(pd.np.nan)
    assert df.df.iloc[0]["ID3"] == False
    assert df.df.iloc[0]["ID4"] == 223372036854777807
    assert math.isclose(df.df.iloc[0]["ID5"], 5.555, abs_tol=0.001)
    assert math.isclose(df.df.iloc[0]["ID6"], 2000.55, abs_tol=0.01)
    assert type(df.df.iloc[0]["ID7"]) == type(pd.NaT)




def test_missing_data(mocker):
    mocker.patch('pandas.Timestamp.utcnow', return_value=pd.Timestamp('2019-07-09 22:32:33.135693+00:00').tz_localize(None))
    test_schema = """
        {
            "namespace": "Dataframe",
            "type": "object",
            "name": "I am a test",
            "fields": [
                {"name":"Id1","type":{"type":"string"}},
                {"name":"Id2","type":{"type":"int"}},
                {"name":"Id3","type":{"type":"boolean"}},
                {"name":"Id4","type":{"type":"long"}},
                {"name":"Id5","type":{"type":"float"}},
                {"name":"Id6","type":{"type":"double"}},
                {"name":"Id7","type":{"type":"string", "logical_type":"datetime"}}
            ]
        }
        """
    test_data = """
        {
                "Id1":"foo",
                "Id2":10,
                "Id3": false,
                "Id4":223372036854777807,
                "Id5":5.555,
                "Id6": 2000.55
        }
        """
    df = Dataframe(SDCDFTypes.PANDAS, test_schema)
    with pytest.raises(Exception):
        df.load_data([json.loads(test_data)])

def test_unexpected_data(mocker):
    mocker.patch('pandas.Timestamp.utcnow', return_value=pd.Timestamp('2019-07-09 22:32:33.135693+00:00').tz_localize(None))
    test_schema = """
        {
            "namespace": "Dataframe",
            "type": "object",
            "name": "I am a test",
            "fields": [
                {"name":"Id1","type":{"type":"string"}},
                {"name":"Id2","type":{"type":"int"}},
                {"name":"Id3","type":{"type":"boolean"}},
                {"name":"Id4","type":{"type":"long"}},
                {"name":"Id5","type":{"type":"float"}},
                {"name":"Id6","type":{"type":"double"}},
                {"name":"Id7","type":{"type":"string", "logical_type":"datetime"}}
            ]
        }
        """
    test_data = """
        {
                "Id1":"foo",
                "Id2":10,
                "Id3": false,
                "Id4":223372036854777807,
                "Id5":5.555,
                "Id6": 2000.55,
                "Id7": "2019-07-09 22:32:33.135693+00:00",
                "Id8" : "cool"
        }
        """

    df = Dataframe(SDCDFTypes.PANDAS, test_schema)
    df.load_data([json.loads(test_data)])
    assert("ID8" not in df.df.columns)


def test_nullable_data(mocker):
    mocker.patch('pandas.Timestamp.utcnow', return_value=pd.Timestamp('2019-07-09 22:32:33.135693+00:00').tz_localize(None))
    test_schema = """
        {
            "namespace": "Dataframe",
            "type": "object",
            "name": "I am a test",
            "fields": [
                {"name":"Id1","type":{"type":"string"}},
                {"name":"Id2","type":{"type":"int"}},
                {"name":"Id3","type":{"type":"boolean"}},
                {"name":"Id4","type":{"type":"long"}},
                {"name":"Id5","type":{"type":"float"}},
                {"name":"Id6","type":{"type":"double"}},
                {"name":"Id7","type":{"type":"string", "logical_type":"datetime"}},
                {"name":"Id9","type":{"type":"int"}, "is_nullable":true}
            ]
        }
        """
    test_data = """
        {
                "Id1":"foo",
                "Id2":10,
                "Id3": false,
                "Id4":223372036854777807,
                "Id5":5.555,
                "Id6": 2000.55,
                "Id7": "2019-07-09 22:32:33.135693+00:00",
                "Id8" : "cool"
        }
        """

    df = Dataframe(SDCDFTypes.PANDAS, test_schema)
    df.load_data([json.loads(test_data)])
    assert("ID9" in df.df.columns)


def test_find_json_columns(mocker):

    test_schema = """
    {
    "namespace": "",
    "type": "object",
    "name": "",
    "country_code": "USA",
    "data_sink": {"type":"snowflake", "database": "", "table_name": "", "schema": ""},
    "data_source": {"type": "api", "base_url": ""},
    "fields": [
      {"name":"CODE","type":{"type":"string"}},
      {"name":"DESCRIPTION","type":{"type":"string"}},
      {"name":"WBS","type":{"type":"string","logical_type":"json"}},
      {"name":"BANKS","type":{"type":"string","logical_type":"json"}},
      {"name":"LASTMODIFIEDAT","type":{"type":"string","logical_type":"datetime"}},
      {"name":"LASTMODIFIEDBY","type":{"type":"string"}},
      {"name":"BUDGETHOURS","type":{"type":"float"}},
      {"name":"BUDGETCOST","type":{"type":"float"}},
      {"name":"STARTDATE","type":{"type":"string", "logical_type":"datetime"}, "is_nullable": true},
      {"name":"FINISHDATE","type":{"type":"string", "logical_type":"datetime"}, "is_nullable": true},
      {"name":"PARENTKEY","type":{"type":"int"}, "is_nullable": true},
      {"name":"STATUS","type":{"type":"string"}},
      {"name":"PROJECT","type":{"type":"string","logical_type":"json"}},
      {"name":"USERFIELDS","type":{"type":"string","logical_type":"json"}},
      {"name":"_METADATA","type":{"type":"string","logical_type":"json"}},
      {"name":"KEY","type":{"type":"int"},"sf_merge_key": true},
      {"name":"_SF_INSERTEDDATETIME","type":{"type":"string","logical_type":"datetime", "add_column": true }}
    ]
    }"""

    df = Dataframe(SDCDFTypes.PANDAS, test_schema)
    assert len(df.json_columns) == 5

