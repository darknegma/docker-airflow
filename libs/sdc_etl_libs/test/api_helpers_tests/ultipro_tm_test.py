import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../../")
from sdc_etl_libs.api_helpers.apis.Ultipro.UltiproTimeManagement import UltiproTimeManagement


def test_no_data(mocker):
    mocker.patch('sdc_etl_libs.api_helpers.API.API.get_credentials',
                 return_value={"tm_username": "foo", "tm_password": "bar"})

    class ReqMock:
        status_code = 200
        content = """{
      "@odata.context": "http://knw12.ulticlock.com/UtmOdataServices/api$metadata#OrgLevel1",
      "@odata.count": 0,
      "value": []
      }"""

    mocker.patch('requests.get', return_value=ReqMock)

    ultipro = UltiproTimeManagement()

    df = ultipro.get_data_from_endpoint('org-level-1', 'OrgLevel1', None, limit_=100)
    assert (df == None)


def test_yes_data(mocker):
    mocker.patch('sdc_etl_libs.api_helpers.API.API.get_credentials',
                 return_value={"tm_username": "foo", "tm_password": "bar"})

    class ReqMock:
        status_code = 200
        content = """{
  "@odata.context": "http://knw12.ulticlock.com/UtmOdataServices/api/$metadata#OrgLevel1",
  "@odata.count": 3,
  "value": [
    {
      "Id": 3,
      "Name": "11000",
      "Description": "Admin"
    },
    {
      "Id": 4,
      "Name": "11006",
      "Description": "CorpDev"
    },
    {
      "Id": 5,
      "Name": "11001",
      "Description": "Finance"
    }
    ]}"""

    mocker.patch('requests.get', return_value=ReqMock)

    ultipro = UltiproTimeManagement()

    df = ultipro.get_data_from_endpoint('org-level-1', 'OrgLevel1', None, limit_=100)
    assert df.df.shape == (3, 4)



