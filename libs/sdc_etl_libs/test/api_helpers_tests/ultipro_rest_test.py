import sys
import os
import json

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../../")
from sdc_etl_libs.api_helpers.apis.Ultipro.UltiproRESTAPIs import UltiproRESTAPIs


def test_no_data(mocker):
    mocker.patch('sdc_etl_libs.api_helpers.API.API.get_credentials',
                 return_value={"username": "foo", "password": "bar",
                               "api_key": "baz"})

    class ReqMock():
        status_code = 200
        content = "[]"

    mocker.patch('requests.get', return_value=ReqMock)

    ultipro = UltiproRESTAPIs()

    df = ultipro.get_employment_details()
    assert (df == None)


def test_paginate_data(mocker):
    mocker.patch('sdc_etl_libs.api_helpers.API.API.get_credentials',
                 return_value={"username": "foo", "password": "bar",
                               "api_key": "baz"})

    class ReqMock():
        def __init__(self, status_, data_):
            self.status_code = status_
            self.content = data_

    def get_mock(url_, **kwargs):
        print(url_)

        page_1 = [
            {
                "companyID": "string",
                "companyCode": "string",
                "companyName": "string",
                "employeeID": "string",
                "jobDescription": "string",
                "payGroupDescription": "string",
                "primaryJobCode": "string",
                "orgLevel1Code": "string",
                "orgLevel2Code": "string",
                "orgLevel3Code": "string",
                "orgLevel4Code": "string",
                "originalHireDate": "2019-06-13T15:06:18.337Z",
                "lastHireDate": "2019-06-13T15:06:18.337Z",
                "fullTimeOrPartTimeCode": "string",
                "primaryWorkLocationCode": "string",
                "languageCode": "string",
                "primaryProjectCode": "string",
                "workPhoneNumber": "string",
                "workPhoneExtension": "string",
                "workPhoneCountry": "string",
                "dateInJob": "2019-06-13T15:06:18.337Z",
                "dateLastWorked": "2019-06-13T15:06:18.337Z",
                "dateOfBenefitSeniority": "2019-06-13T15:06:18.337Z",
                "dateOfSeniority": "2019-06-13T15:06:18.337Z",
                "deductionGroupCode": "string",
                "earningGroupCode": "string",
                "employeeTypeCode": "string",
                "employeeStatusCode": "string",
                "employeeNumber": "string",
                "jobChangeReasonCode": "string",
                "jobTitle": "string",
                "leaveReasonCode": "string",
                "supervisorID": "string",
                "supervisorFirstName": "string",
                "supervisorLastName": "string",
                "autoAllocate": "string",
                "clockCode": "string",
                "dateLastPayDatePaid": "2019-06-13T15:06:18.337Z",
                "dateOfEarlyRetirement": "2019-06-13T15:06:18.337Z",
                "dateOfLocalUnion": "2019-06-13T15:06:18.337Z",
                "dateOfNationalUnion": "2019-06-13T15:06:18.337Z",
                "dateOfRetirement": "2019-06-13T15:06:18.337Z",
                "dateOfSuspension": "2019-06-13T15:06:18.337Z",
                "dateOfTermination": "2019-06-13T15:06:18.337Z",
                "datePaidThru": "2019-06-13T15:06:18.337Z",
                "statusStartDate": "2019-06-13T15:06:18.337Z",
                "hireSource": "string",
                "isAutoAllocated": "string",
                "isAutopaid": "string",
                "isMultipleJob": "string",
                "jobGroupCode": "string",
                "mailstop": "string",
                "okToRehire": "string",
                "payGroup": "string",
                "payPeriod": "string",
                "plannedLeaveReason": "string",
                "positionCode": "string",
                "salaryOrHourly": "string",
                "scheduledAnnualHrs": 0,
                "scheduledFTE": 0,
                "scheduledWorkHrs": 0,
                "shift": "string",
                "shiftGroup": "string",
                "termReason": "string",
                "terminationReasonDescription": "string",
                "termType": "string",
                "timeclockID": "string",
                "unionLocal": "string",
                "unionNational": "string",
                "weeklyHours": 0,
                "dateTimeCreated": "2019-06-13T15:06:18.337Z",
                "dateTimeChanged": "2019-06-13T15:06:18.337Z"
            }
        ]
        page_2 = [
            {
                "companyID": "fpo",
                "companyCode": "bar",
                "companyName": "baz",
                "employeeID": "hhshw66222w2w",
                "jobDescription": "coolest",
                "payGroupDescription": "string",
                "primaryJobCode": "fake",
                "orgLevel1Code": "stuff",
                "orgLevel2Code": "al;ways",
                "orgLevel3Code": "helps",
                "orgLevel4Code": "with",
                "originalHireDate": "2019-07-13T15:06:18.337Z",
                "lastHireDate": "2019-07-13T15:06:18.337Z",
                "fullTimeOrPartTimeCode": "testing",
                "primaryWorkLocationCode": "string",
                "languageCode": "string",
                "primaryProjectCode": "string",
                "workPhoneNumber": "string",
                "workPhoneExtension": "string",
                "workPhoneCountry": "string",
                "dateInJob": "2019-06-13T15:06:18.337Z",
                "dateLastWorked": "2019-06-13T15:06:18.337Z",
                "dateOfBenefitSeniority": "2019-06-13T15:06:18.337Z",
                "dateOfSeniority": "2019-06-13T15:06:18.337Z",
                "deductionGroupCode": "string",
                "earningGroupCode": "string",
                "employeeTypeCode": "string",
                "employeeStatusCode": "string",
                "employeeNumber": "string",
                "jobChangeReasonCode": "string",
                "jobTitle": "string",
                "leaveReasonCode": "string",
                "supervisorID": "string",
                "supervisorFirstName": "string",
                "supervisorLastName": "string",
                "autoAllocate": "string",
                "clockCode": "string",
                "dateLastPayDatePaid": "2019-06-13T15:06:18.337Z",
                "dateOfEarlyRetirement": "2019-06-13T15:06:18.337Z",
                "dateOfLocalUnion": "2019-06-13T15:06:18.337Z",
                "dateOfNationalUnion": "2019-06-13T15:06:18.337Z",
                "dateOfRetirement": "2019-06-13T15:06:18.337Z",
                "dateOfSuspension": "2019-06-13T15:06:18.337Z",
                "dateOfTermination": "2019-06-13T15:06:18.337Z",
                "datePaidThru": "2019-06-13T15:06:18.337Z",
                "statusStartDate": "2019-06-13T15:06:18.337Z",
                "hireSource": "string",
                "isAutoAllocated": "string",
                "isAutopaid": "string",
                "isMultipleJob": "string",
                "jobGroupCode": "string",
                "mailstop": "string",
                "okToRehire": "string",
                "payGroup": "string",
                "payPeriod": "string",
                "plannedLeaveReason": "string",
                "positionCode": "string",
                "salaryOrHourly": "string",
                "scheduledAnnualHrs": 0,
                "scheduledFTE": 0,
                "scheduledWorkHrs": 0,
                "shift": "string",
                "shiftGroup": "string",
                "termReason": "string",
                "terminationReasonDescription": "string",
                "termType": "string",
                "timeclockID": "string",
                "unionLocal": "string",
                "unionNational": "string",
                "weeklyHours": 0,
                "dateTimeCreated": "2019-06-13T15:06:18.337Z",
                "dateTimeChanged": "2019-06-13T15:06:18.337Z"
            }
        ]
        if url_ == 'https://service5.ultipro.com/api/personnel/v1/employment-details?page=1&per_page=100':
            return ReqMock(200, json.dumps(page_1))
        elif url_ == 'https://service5.ultipro.com/api/personnel/v1/employment-details?page=2&per_page=100':
            return ReqMock(200, json.dumps(page_2))
        else:
            return ReqMock(200, "[]")

    mock_get = mocker.patch('requests.get', new_callable=lambda: get_mock)

    ultipro = UltiproRESTAPIs()

    df = ultipro.get_employment_details()
    assert len(df.df) == 2
