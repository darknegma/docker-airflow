import datetime
import sys
import os
import json

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../../")
from sdc_etl_libs.api_helpers.apis.TimeControl.TimeControl import TimeControl


def test_no_data(mocker):
    mocker.patch('sdc_etl_libs.api_helpers.API.API.get_credentials',
                 return_value={"apikey": "baz"})

    class ReqMock():
        status_code = 200
        content = "[]"

    mocker.patch('requests.get', return_value=ReqMock)

    timecontrol = TimeControl()

    df = timecontrol.get_data('employees', 'employees', filter_=None, limit_=1000)
    assert (df == None)

def test_endpoint_processing(mocker):

    mocker.patch('sdc_etl_libs.api_helpers.API.API.get_credentials',
                 return_value={"apikey": "blah"})

    class ReqMock():
        def __init__(self, status_, data_):
            self.status_code = status_
            self.content = data_

    mocker.patch(
        'sdc_etl_libs.api_helpers.apis.TimeControl.TimeControl.TimeControl.process_endpoint',
        return_value=
        [
            {
                "_metadata": {
                    "links": [
                        {
                            "id": "9",
                            "rel": "self",
                            "href": "/api/v1/languages/9",
                            "code": "Ceština"
                        }
                    ]
                },
                "Key": 9,
                "Name": "Ceština",
                "Description": "Czech",
                "Culture": "cs"
            },
            {
                "_metadata": {
                    "links": [
                        {
                            "id": "5",
                            "rel": "self",
                            "href": "/api/v1/languages/5",
                            "code": "Dansk"
                        }
                    ]
                },
                "Key": 5,
                "Name": "Dansk",
                "Description": "Danish",
                "Culture": "da"
            },
            {
                "_metadata": {
                    "links": [
                        {
                            "id": "9",
                            "rel": "self",
                            "href": "/api/v1/languages/9",
                            "code": "Ceština"
                        }
                    ]
                },
                "Key": 9,
                "Name": "Ceština",
                "Description": "Czech",
                "Culture": "cs"
            },
            {
                "_metadata": {
                    "links": [
                        {
                            "id": "5",
                            "rel": "self",
                            "href": "/api/v1/languages/5",
                            "code": "Dansk"
                        }
                    ]
                },
                "Key": 5,
                "Name": "Dansk",
                "Description": "Danish",
                "Culture": "da"
            }
        ]
    )

    timecontrol = TimeControl()

    df = timecontrol.get_data('languages', 'languages', filter_=None,
                              limit_=1000)
    assert len(df.df) == 4


def test_pagination_of_data(mocker):
    mocker.patch('sdc_etl_libs.api_helpers.API.API.get_credentials',
                 return_value={"apikey": "blah"})

    class ReqMock():
        def __init__(self, status_, data_):
            self.status_code = status_
            self.content = data_

    def get_mock(url_, **kwargs):
        print(url_)

        page_1 = [
    {
        "_metadata": {
            "links": [
                {
                    "id": "9",
                    "rel": "self",
                    "href": "/api/v1/languages/9",
                    "code": "Ceština"
                }
            ]
        },
        "Key": 9,
        "Name": "Ceština",
        "Description": "Czech",
        "Culture": "cs"
    },
    {
        "_metadata": {
            "links": [
                {
                    "id": "5",
                    "rel": "self",
                    "href": "/api/v1/languages/5",
                    "code": "Dansk"
                }
            ]
        },
        "Key": 5,
        "Name": "Dansk",
        "Description": "Danish",
        "Culture": "da"
    }
        ]
        page_2 = [
    {
        "_metadata": {
            "links": [
                {
                    "id": "9",
                    "rel": "self",
                    "href": "/api/v1/languages/9",
                    "code": "Ceština"
                }
            ]
        },
        "Key": 9,
        "Name": "Ceština",
        "Description": "Czech",
        "Culture": "cs"
    },
    {
        "_metadata": {
            "links": [
                {
                    "id": "5",
                    "rel": "self",
                    "href": "/api/v1/languages/5",
                    "code": "Dansk"
                }
            ]
        },
        "Key": 5,
        "Name": "Dansk",
        "Description": "Danish",
        "Culture": "da"
    }
        ]
        if url_ == 'https://smiledirectclub.timecontrol.net/api/v1/languages?$skip=0&$top=2':
            return ReqMock(200, json.dumps(page_1))
        elif url_ == 'https://smiledirectclub.timecontrol.net/api/v1/languages?$skip=2&$top=2':
            return ReqMock(200, json.dumps(page_2))
        else:
            return ReqMock(200, "[]")

    mock_get = mocker.patch('requests.get', new_callable=lambda: get_mock)

    timecontrol = TimeControl()

    df = timecontrol.get_data('languages', 'languages', filter_=None,
                              limit_=2)
    assert len(df.df) == 4

def test_key_filter(mocker):

    timecontrol = TimeControl()

    filter = timecontrol.get_key_filter(start_key_=25, json_key_path_="Key")

    assert filter == 'Key ge 25'

def test_key_filter_with_path(mocker):

    timecontrol = TimeControl()

    filter = timecontrol.get_key_filter(start_key_=25, json_key_path_="Employee/Key")

    assert filter == 'Employee/Key ge 25'

def test_daily_filter(mocker):

    datetime_ = datetime.datetime(2019, 10, 14, 10, 55, 5, 990098)

    timecontrol = TimeControl()

    filter = timecontrol.get_daily_filter(datetime_=datetime_, days_=3,
                                          filter_field_list_=['LastModifiedAt'])

    assert filter == "(LastModifiedAt ge DateTime'2019-10-11T00:00:00' and LastModifiedAt lt DateTime'2019-10-14T00:00:00')"

def test_daily_filter_with_multiple_fields(mocker):

    datetime_ = datetime.datetime(2019, 10, 14, 10, 55, 5, 990098)

    timecontrol = TimeControl()

    filter = timecontrol.get_daily_filter(datetime_=datetime_, days_=3,
                                          filter_field_list_=['LastModifiedAt','CreatedDate'])

    assert filter == "(LastModifiedAt ge DateTime'2019-10-11T00:00:00' and LastModifiedAt lt DateTime'2019-10-14T00:00:00') " \
                     "or (CreatedDate ge DateTime'2019-10-11T00:00:00' and CreatedDate lt DateTime'2019-10-14T00:00:00')"
