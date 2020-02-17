
from enum import Enum


class DEEMails(Enum):
    DE_DL = 'sdcde@smiledirectclub.com'
    DE_TEST = ''
    ANALYTICS_DL = 'analytics@smiledirectclub.com'


dag_emails = {
    # Data Engineering
    "data-eng": [DEEMails.DE_DL.value],
    "data-eng-test": [DEEMails.DE_TEST.value],

    # Logistics
    "etl-barrett": [DEEMails.DE_DL.value, "cullen.sorrels@smiledirectclub.com"],
    "etl-fedex": [DEEMails.DE_DL.value, "cullen.sorrels@smiledirectclub.com"],
    "etl-tnt": [DEEMails.DE_DL.value, "cullen.sorrels@smiledirectclub.com"],

    # Contact Center
    "etl-five9-daily": [DEEMails.DE_DL.value, "geoffrey.bent@smiledirectclub.com", "anita.gandhi@smiledirectclub.com", "erik.vega@smiledirectclub.com"],
    "etl-five9-hourly": [DEEMails.DE_DL.value, "geoffrey.bent@smiledirectclub.com", "anita.gandhi@smiledirectclub.com", "erik.vega@smiledirectclub.com"],
    "etl-five9-weekly": [DEEMails.DE_DL.value, "geoffrey.bent@smiledirectclub.com", "anita.gandhi@smiledirectclub.com", "erik.vega@smiledirectclub.com"],

    # Finance
    "etl-hfd-reports": [DEEMails.DE_DL.value, "howell.zheng@smiledirectclub.com", "megan.cutcher@smiledirectclub.com", "paul.howell@smiledirectclub.com"],
    "etl-hfd": [DEEMails.DE_DL.value, "howell.zheng@smiledirectclub.com", "megan.cutcher@smiledirectclub.com", "paul.howell@smiledirectclub.com"],
    "etl-stripe-api": [DEEMails.DE_DL.value, "howell.zheng@smiledirectclub.com", "paul.howell@smiledirectclub.com"],
    "finance-xf-smilepay-data": [DEEMails.DE_DL.value, "howell.zheng@smiledirectclub.com", "paul.howell@smiledirectclub.com"],

    # HRIS
    "tap-ultipro": [DEEMails.DE_TEST.value],
    "etl-icims": [DEEMails.DE_DL.value, "geoffrey.bent@smiledirectclub.com", "drew.douthit@smiledirectclub.com"],
    "ultipro": [DEEMails.DE_DL.value, "geoffrey.bent@smiledirectclub.com", "drew.douthit@smiledirectclub.com"],
    "ultipro-time-management": [DEEMails.DE_DL.value, "geoffrey.bent@smiledirectclub.com", "drew.douthit@smiledirectclub.com"],
    "timecontrol-api": [DEEMails.DE_DL.value, "howell.zheng@smiledirectclub.com", "paul.howell@smiledirectclub.com"],
    
    # Marketing
    "dataxu": [DEEMails.DE_DL.value, "tanner.hopkins@smiledirectclub.com"],
    "etl-exacttarget-api": [DEEMails.DE_DL.value, "tanner.hopkins@smiledirectclub.com"],
    "etl-horizon-web-data": [DEEMails.DE_DL.value, "tanner.hopkins@smiledirectclub.com"],
    "etl-logic-plum": [DEEMails.DE_DL.value, "drew.douthit@smiledirectclub.com", "alexandra.benya@smiledirectclub.com"],
    "etl-neustar-email-data": [DEEMails.DE_DL.value, "tanner.hopkins@smiledirectclub.com"],
    "etl-neustar-web-data": [DEEMails.DE_DL.value],
    "etl-tealium-status-data": [DEEMails.DE_DL.value],
    "etl-podium-api": [DEEMails.DE_DL.value, "connie.cape@smiledirectclub.com"],
    "tap-bitly": [DEEMails.DE_DL.value],
    "tap-friendbuy": [DEEMails.DE_DL.value],
    "tap-podium": [DEEMails.DE_DL.value, "connie.cape@smiledirectclub.com"],
    "tap-qualtrics": [DEEMails.DE_DL.value],
    "tap-typeform": [DEEMails.DE_DL.value],
    "etl-newrelic-useragent-browser-performance": [DEEMails.DE_DL.value],
    "google-search-ads-360": [DEEMails.DE_DL.value, "connie.cape@smiledirectclub.com"]

}