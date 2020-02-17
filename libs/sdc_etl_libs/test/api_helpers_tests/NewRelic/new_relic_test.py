import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../../../")
from sdc_etl_libs.api_helpers.apis.NewRelic.NewRelic import NewRelic



def test_flatten_metrics_data():

    dict_to_flatten = [
              {
                "name": "EndUser/UserAgent/Mobile/Browser",
                "timeslices": [
                  {
                    "from": "2015-10-01T01:00:00+00:00",
                    "to": "2015-10-04T01:00:00+00:00",
                    "values": {
                      "average_fe_response_time": 0,
                      "average_be_response_time": 0
                    }
                  }
                ]
              }
            ]

    expected_result = [{'name': 'EndUser/UserAgent/Mobile/Browser',
                        'from': '2015-10-01T01:00:00+00:00',
                        'to': '2015-10-04T01:00:00+00:00',
                        'average_fe_response_time': 0,
                        'average_be_response_time': 0}]


    new_relic = NewRelic()

    flattened_data = new_relic.flatten_metrics_data(dict_to_flatten)

    assert flattened_data == expected_result

def test_get_date_filter():

    date_to_check = '2019-11-06 09:50:56.172035'
    expected_result = 'from=2019-11-03T01:00:00+00:00&to=2019-11-06T01:00:00+00:00&period=3600'

    new_relic = NewRelic()

    url_date_filter = new_relic.get_date_filter(datetime_=date_to_check, days_=3, periods_=3600)

    assert url_date_filter == expected_result

def test_get_fields_filter():

    names_to_check = ['EndUser/UserAgent/Mobile/Browser&names', 'EndUser/UserAgent/Tablet/Browser']
    values_to_check = ['average_response_time', 'average_fe_response_time', 'average_be_response_time']
    expected_result = 'names[]=EndUser/UserAgent/Mobile/Browser&names&names[]=EndUser/UserAgent/Tablet/Browser&values[]=average_response_time&values[]=average_fe_response_time&values[]=average_be_response_time'

    new_relic = NewRelic()

    url_fields_filter = new_relic.get_fields_filter(names_=names_to_check, values_=values_to_check)

    assert url_fields_filter == expected_result


def test_get_metrics_data(mocker):
    mocker.patch('sdc_etl_libs.api_helpers.API.API.get_credentials',
                 return_value={"X-API-Key": "blah"})

    mocker.patch('sdc_etl_libs.sdc_file_helpers.SDCFileHelpers.SDCFileHelpers.get_file_path',
                 return_value=os.path.join(
                os.path.dirname(os.path.abspath(__file__)), 'newrelic-test-schema-1.json')
                 )

    class ReqMock():
        def __init__(self, status_, data_):
            self.status_code = status_
            self.content = data_

    mocker.patch(
        'sdc_etl_libs.api_helpers.apis.NewRelic.NewRelic.NewRelic.process_metrics_endpoint',
        return_value=[{'name': 'EndUser/UserAgent/Tablet/Browser',
                       'timeslices': [{'from': '2019-11-03T01:00:00+00:00',
                                       'to': '2019-11-03T02:00:00+00:00',
                                       'values': {
                                           'average_response_time': 13800,
                                           'average_fe_response_time': 12500,
                                           'average_be_response_time': 1320}},
                                      {'from': '2019-11-03T02:00:00+00:00',
                                       'to': '2019-11-03T03:00:00+00:00',
                                       'values': {
                                           'average_response_time': 12300,
                                           'average_fe_response_time': 11100,
                                           'average_be_response_time': 1250}}]
                       }]
    )

    new_relic = NewRelic()

    df = new_relic.get_metrics_data("useragent-browser-performance",
                    date_filter_=new_relic.get_date_filter(datetime_='2019-11-06 09:50:56.172035', days_=3),
                    fields_filter_=new_relic.get_fields_filter(names_=["EndUser/UserAgent/Mobile/Browser&names", "EndUser/UserAgent/Tablet/Browser", "EndUser/UserAgent/Desktop/Browser"],
                    values_=["average_response_time", "average_fe_response_time"])
                    )
    assert len(df.df) == 2