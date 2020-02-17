import sys
import os
import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../../")
from sdc_etl_libs.api_helpers.apis.Podium.Podium import Podium


def test_get_date_range_filter(mocker):

    mocker.patch('sdc_etl_libs.api_helpers.API.API.get_credentials',
                 return_value={"key": "blah"})

    assert_result = 'fromDate=2019-09-20&toDate=2019-09-24'

    podium = Podium()

    datetime_ = datetime.datetime(2019, 9, 23, 10, 55, 5, 990098)

    test_result = podium.get_date_range_filter(datetime_, 3)

    assert assert_result == test_result

def test_endpoint_processing(mocker):

    mocker.patch('sdc_etl_libs.api_helpers.API.API.get_credentials',
                 return_value={"key": "blah"})

    class ReqMock():
        def __init__(self, status_, data_):
            self.status_code = status_
            self.content = data_

    mocker.patch(
        'sdc_etl_libs.api_helpers.apis.Podium.Podium.Podium.process_endpoint',
        return_value=
            [
        {
            "locationId": 39517,
            "locationName": "Shop#93: Smile Direct Club: San Antonio"
        },
        {
            "locationId": 39476,
            "locationName": "Shop#21: Smile Direct Club: Manhattan- Flatiron District"
        },
        {
            "locationId": 39519,
            "locationName": "Shop#47: Smile Direct Club: San Francisco- Financial District"
        },
        {
            "locationId": 46004,
            "locationName": "Shop#311: Smile Direct Club: Walnut Creek"
        },
        {
            "locationId": 39447,
            "locationName": "Shop#60: Smile Direct Club: Manhattan Beach"
        },
        {
            "locationId": 39478,
            "locationName": "Shop#415: Smile Direct Club: Manhattan - Upper East Side"
        },
        {
            "locationId": 47946,
            "locationName": "Shop#375: Smile Direct Club - West Des Moines"
        },
        {
            "locationId": 39340,
            "locationName": "Shop#168: Smile Direct Club: Austin- Bee Cave"
        },
        {
            "locationId": 39487,
            "locationName": "Shop#110: Smile Direct Club: Ontario- Waterside"
        },
        {
            "locationId": 39429,
            "locationName": "Shop#40: Smile Direct Club: Jacksonville"
        },
        {
            "locationId": 39389,
            "locationName": "Shop#234: Smile Direct Club: Chicago- River North"
        },
        {
            "locationId": 39464,
            "locationName": "Shop#87: Smile Direct Club: Minneapolis- Minnetonka"
        },
        {
            "locationId": 39526,
            "locationName": "Shop#133: Smile Direct Club: Sarasota"
        },
        {
            "locationId": 41280,
            "locationName": "Shop#237: Smile Direct Club: Toronto- Thornhill"
        },
        {
            "locationId": 41279,
            "locationName": "Shop#238: Smile Direct Club: Springfield - Agwam"
        },
        {
            "locationId": 41277,
            "locationName": "Shop#239: Smile Direct Club: Calgary- Macleod"
        },
        {
            "locationId": 41276,
            "locationName": "Shop#240: Smile Direct Club: Little Rock"
        },
        {
            "locationId": 41274,
            "locationName": "Shop#241: Smile Direct Club: Woodbury- Tamarack"
        },
        {
            "locationId": 54337,
            "locationName": "Shop#585: Smile Direct Club -Woodland Hills inside CVS"
        },
        {
            "locationId": 56711,
            "locationName": "AUS - Shop#:677: Smile Direct Club - North Sydney"
        },
        {
            "locationId": 39434,
            "locationName": "Shop#222: Smile Direct Club: Las Vegas- Apache"
        },
        {
            "locationId": 55991,
            "locationName": "Shop#622: Smile Direct Club: Toronto- York"
        },
        {
            "locationId": 39380,
            "locationName": "Shop#67: Smile Direct Club: Milwaukee- Brookfield"
        },
        {
            "locationId": 39430,
            "locationName": "Shop#97: Smile Direct Club: King of Prussia"
        },
        {
            "locationId": 39336,
            "locationName": "Shop# 386 Smile Direct Club- Asheville"
        }]
    )

    podium = Podium()

    df = podium.get_locations()
    assert len(df.df) == 25