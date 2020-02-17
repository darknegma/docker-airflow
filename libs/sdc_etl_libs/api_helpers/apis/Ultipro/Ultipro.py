import logging
import backoff
import requests
from ast import literal_eval
from zeep import Client as Zeep
from zeep import xsd
from sdc_etl_libs.api_helpers.API import API

logging.basicConfig(level=logging.INFO)


class Ultipro(API):

    def __init__(self):
        self.credentials = self.get_credentials("aws_secrets", "ultipro")
        self.base_url = ""

    def process_endpoint(self):
        pass

    def get_daily_filter(self):
        raise Exception("Do not use base class get_daily_filter function.")

    def rest_authenticate(self, username_key_, password_key_):
        """
        Authentication for Ultipro REST API.
        :param username_key_: Secrets dict key for username
        :param password_key_: Secrets dict key for password
        :return:
        """

        self.auth = literal_eval(f"('{self.credentials[username_key_]}','{self.credentials[password_key_]}')")

    @staticmethod
    def soap_backoff_handler(details):
        """
        Message formatting function for Backoff messages.
        :return: Message for logger.
        """
        logging.warning("Backing off {wait:0.1f} seconds after {tries} tries "
              "calling function {target}".format(**details))

    @backoff.on_exception(backoff.expo, requests.exceptions.HTTPError,
                          max_tries=8, on_backoff=soap_backoff_handler)
    def soap_authenticate(self):
        """
        Authentication for Ultipro SOAP connection.
        :return: None
        """

        login_header = {
            'UserName': self.credentials["soap_username"],
            'Password': self.credentials["soap_password"],
            'ClientAccessKey': self.credentials["api_key"],
            'UserAccessKey': self.credentials["soap_user_access_key"]
        }

        zeep_client = Zeep(f"{self.base_url}LoginService")
        result = zeep_client.service.Authenticate(_soapheaders=login_header)
        self.token = result['Token']

        # Create xsd ComplexType header -
        # http://docs.python-zeep.org/en/master/headers.html
        header = xsd.ComplexType([
            xsd.Element(
                '{http://www.ultimatesoftware.com/foundation/authentication'
                '/ultiprotoken}UltiProToken',
                xsd.String()),
            xsd.Element(
                '{http://www.ultimatesoftware.com/foundation/authentication'
                '/clientaccesskey}ClientAccessKey',
                xsd.String()),
        ])

        # Add authenticated header to client object
        self.session_header = header(UltiProToken=self.token,
                                     ClientAccessKey=self.credentials["api_key"])