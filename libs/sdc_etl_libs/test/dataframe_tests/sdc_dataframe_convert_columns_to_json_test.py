import sys
import math
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../../")
from sdc_etl_libs.sdc_dataframe.Dataframe import *
import pandas as pd
import numpy as np
import json
import pytest


def test_convert_columns_to_json_replace_newline_characters():
    """
    Ensure when converting a string to JSON that incidental newline / carriage
    return characters in values are double-escaped.

    """

    test_schema = """
    {
    "namespace": "",
    "type": "object",
    "name": "",
    "country_code": "USA",
    "data_sink": {"type":"snowflake", "database": "", "table_name": "", "schema": ""},
    "data_source": {"type": "api", "base_url": ""},
    "fields": [
      {"name":"COL1","type":{"type":"string","logical_type":"json"}}
    ]
    }"""

    df = Dataframe(SDCDFTypes.PANDAS, test_schema)
    df.load_data([
        {'COL1': {'Notes': 'Microsoft EA meeting; (Wed)\nFriday - FireDrill'}},
        {'COL1': {'Notes': 'Microsoft EA meeting; (Wed)Friday\r - FireDrill'}},
        {'COL1': {'Notes': 'Microsoft EA meeting; (Wed)\n\nFriday - FireDrill'}}])

    df.convert_columns_to_json()

    assert df.df["COL1"][0] == '{"Notes": "Microsoft EA meeting; (Wed)\\\\nFriday - FireDrill"}'
    assert df.df["COL1"][1] == '{"Notes": "Microsoft EA meeting; (Wed)Friday\\\\r - FireDrill"}'
    assert df.df["COL1"][2] == '{"Notes": "Microsoft EA meeting; (Wed)\\\\n\\\\nFriday - FireDrill"}'
