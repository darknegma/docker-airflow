import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../../")
from sdc_etl_libs.sdc_dataframe.Dataframe import Dataframe, SDCDFTypes
import pytest

def test_drop_columns():

    test_schema = """
        {
            "namespace": "Dataframe",
            "type": "object",
            "name": "I am a test",
            "fields": [
                {"name":"Id1","type":{"type":"string"}},
                {"name":"Id2","type":{"type":"int"}},
                {"name":"Id3","type":{"type":"boolean"}},
                {"name":"Id4","type":{"type":"string"}},
                {"name":"Id5","type":{"type":"float"}},
                {"name":"Id6","type":{"type":"string"}},
                {"name":"Id7","type":{"type":"string", "logical_type":"datetime"}}
            ]
        }
        """

    df = Dataframe(SDCDFTypes.PANDAS, test_schema)

    df.load_data([{
        "Id1": "string",
        "Id2": 5,
        "Id3": True,
        "Id4": "string",
        "Id5": 5.5,
        "Id6": "string",
        "Id7": "2019-06-13T15:06:18.337Z",
    }])

    assert len(df.df.columns) == 7

    # Test dropping 1 column
    df.drop_columns(["ID1"])
    assert "ID1" not in df.df.columns.tolist()

    # Test dropping multiple columns at once
    df.drop_columns(["ID3", "ID7"])
    assert "ID3" not in df.df.columns.tolist()
    assert "ID7" not in df.df.columns.tolist()

    # Test function fails when argument passed is not a list
    with pytest.raises(Exception):
        df.drop_columns("ID1")

def test_fill_in_column():

    test_schema = """
            {
                "namespace": "Dataframe",
                "type": "object",
                "name": "I am a test",
                "fields": [
                    {"name":"Id1","type":{"type":"string"}},
                    {"name":"Id2","type":{"type":"string", "add_column": true }}
                ]
            }
            """

    df = Dataframe(SDCDFTypes.PANDAS, test_schema)

    df.load_data([
        {"Id1": "string", "Id2": None},
        {"Id1": "string", "Id2": None},
        {"Id1": "string", "Id2": None},
        {"Id1": "string", "Id2": None},
        {"Id1": "string", "Id2": None}])

    assert df.df["ID2"].isnull().all() == True

    df.fill_in_column(column_name_="ID2", column_value_="Cat", create_column_=False)

    assert df.df["ID2"].all() == 'Cat'

    df.fill_in_column(column_name_="ID2", column_value_="Dog", create_column_=False)

    assert df.df["ID2"].all() == 'Dog'

    with pytest.raises(Exception):
        df.fill_in_column(column_name_="ID4", column_value_="Dog",
                          create_column_=False)

    df.fill_in_column(column_name_="ID4", column_value_="Dog", create_column_=True)

    assert df.df["ID4"].all() == 'Dog'
