
import logging


class APIFactory:

    @staticmethod
    def get_api(api_name_):
        """
        Generates an instance of a requested API class.
        :param api_name_: String. Name of API class to generate from available_apis dict.
        :return: API class instance.
        """

        if api_name_ in available_apis.keys():
            return available_apis[api_name_]()
        else:
            logging.exception(f"{api_name_} is not a valid API option.")


def generate_newrelic():
    try:
        from sdc_etl_libs.api_helpers.apis.NewRelic.NewRelic import NewRelic
        return NewRelic()
    except Exception as e:
        logging.error(f"API Factory could not generate an NewRelic API class. {e}")


def generate_truevault():
    try:
        from sdc_etl_libs.api_helpers.apis.TrueVault.TrueVault import TrueVault
        return TrueVault()
    except Exception as e:
        logging.error(f"API Factory could not generate an TrueVault API class. {e}")


def generate_timecontrol():
    try:
        from sdc_etl_libs.api_helpers.apis.TimeControl.TimeControl import TimeControl
        return TimeControl()
    except Exception as e:
        logging.error(f"API Factory could not generate an TimeControl API class. {e}")


def generate_exacttarget():
    try:
        from sdc_etl_libs.api_helpers.apis.ExactTarget.ExactTarget import ExactTarget
        return ExactTarget()
    except Exception as e:
        logging.error(f"API Factory could not generate an ExactTarget API class. {e}")


def generate_podium():
    try:
        from sdc_etl_libs.api_helpers.apis.Podium.Podium import Podium
        return Podium()
    except Exception as e:
        logging.error(f"API Factory could not generate an Podium API class. {e}")


def generate_ultiprorestapis():
    try:
        from sdc_etl_libs.api_helpers.apis.Ultipro.UltiproRESTAPIs import UltiproRESTAPIs
        return UltiproRESTAPIs()
    except Exception as e:
        logging.error(f"API Factory could not generate an UltiproRESTAPIs API class. {e}")


def generate_ultiprotimemanagement():
    try:
        from sdc_etl_libs.api_helpers.apis.Ultipro.UltiproTimeManagement import UltiproTimeManagement
        return UltiproTimeManagement()
    except Exception as e:
        logging.error(f"API Factory could not generate an UltiproTimeManagement API class. {e}")


def generate_ultiproraas():
    try:
        from sdc_etl_libs.api_helpers.apis.Ultipro.UltiproRaaS import UltiproRaaS
        return UltiproRaaS()
    except Exception as e:
        logging.error(f"API Factory could not generate an UltiproRaaS API class. {e}")


def generate_ultiproservices():
    try:
        from sdc_etl_libs.api_helpers.apis.Ultipro.UltiproServices import UltiproServices
        return UltiproServices()
    except Exception as e:
        logging.error(f"API Factory could not generate an UltiproServices API class. {e}")


def generate_searchads360():
    try:
        from sdc_etl_libs.api_helpers.apis.SearchAds360.SearchAds360 import SearchAds360
        return SearchAds360()
    except Exception as e:
        logging.error(f"API Factory could not generate an SearchAds360 API class. {e}")


available_apis = {
        'podium': generate_podium,
        'exacttarget': generate_exacttarget,
        'ultipro-restapis': generate_ultiprorestapis,
        'ultipro-timemanagement': generate_ultiprotimemanagement,
        'ultipro-services': generate_ultiproservices,
        'ultipro-raas': generate_ultiproraas,
        'timecontrol': generate_timecontrol,
        'new-relic': generate_newrelic,
        'truevault': generate_truevault,
        'searchads360': generate_searchads360
}