import sys
import os
import json
from datetime import datetime
from dateutil import parser


sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../../")
from sdc_etl_libs.api_helpers.apis.ExactTarget.ExactTarget import ExactTarget


def test_filter_string(mocker):
    et = ExactTarget()
    date_string = '2018-04-29T17:45:25Z'

    date_property_ = "test"
    from_dt = "2018-04-29T17:40:25"
    to_dt = "2018-04-29T17:45:25"

    from_filter = {'Property': date_property_,
                   'SimpleOperator': 'greaterThan',
                   'DateValue': from_dt}
    to_filter = {'Property': date_property_,
                 'SimpleOperator': 'lessThanOrEqual',
                 'DateValue': to_dt}
    date_filter = {'LeftOperand': from_filter,
                   'LogicalOperator': 'AND',
                   'RightOperand': to_filter}

    test_filter = et.get_filter_for_last_n_minutes("test",5,date_string)

    assert date_filter == test_filter

