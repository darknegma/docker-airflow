
import logging
# Try to import in Airflow environemnt for get_credentials() use
try:
    from airflow.models import Variable
except:
    pass
# Try to import in Glue enivornment for get_credentials() use
try:
    from sdc_etl_libs.aws_helpers.aws_helpers import AWSHelpers
except Exception as e:
    raise Exception(e)


class API(object):
    
    base_url = None
    credentials = None
    schema_name = None

    def __init__(self):
        logging.info("Base constructor.")
        raise Exception("Base constructor.")

    def get_credentials(self, source_, aws_secret_id_=None,
                        aws_secret_region_='us-east-2', airflow_vars_=None):
        """
        @param source_: Where to grab credentials from (Secrets Manager, Airflow, etc.)
            if source_ is 'aws_secrets', pass the following arguments:
                @param aws_secret_id_: AWS Secret Name
                @param aws_secret_region_: AWS region. Defaults to us-east-2.
            if source_ is 'airflow, pass the following argruments:
                @param airflow_vars_: List of Airflow variables
        @return: Dictionary of credentials
        """

        if source_ == 'aws_secrets':
            if aws_secret_id_:
                try:
                    creds = AWSHelpers.get_secrets(secret_id_=aws_secret_id_,
                                                   region_=aws_secret_region_)

                except Exception as e:
                    logging.error(str(e))
                    raise Exception("Failed to get credentials.")

                return creds
            else:
                raise Exception("aws_secret_id_ not provided.")

        elif source_ == 'airflow':
            if airflow_vars_:
                creds = {}
                if isinstance(airflow_vars_, list):
                    for var in airflow_vars_:
                        try:
                            creds.update({var:Variable.get(var)})
                        except Exception as e:
                            logging.exception(f"Error fetching variable '{var}'")
                    return creds
                else:
                    raise Exception("Airflow variable list must be passed as a list.")
            else:
                raise Exception("No Airflow variable list passed.")

        else:
            raise Exception(f"'{source_}' is an invalid credential source type.")

    def get_google_service_credentials(self, service_name_, version_, certificate_, scopes_=None):
        """
        Generates the proper credentials for Google service account based on provided certificate.
        :param service_name_: String. Name of the service account service (ex. 'doubleclicksearch'). List of
            services can be found here: https://developers.google.com/apis-explorer.
        :param version_: String. Version of the service account service (ex. 'v2').
        :param certificate_: Dict. Dictionary containing credentials/certificate information needed by
            service account.
        :param scopes_: List. Credentials/certificates set of scopes.
        :return: Google service handle.
        """

        from google.oauth2 import service_account
        import googleapiclient.discovery
        credentials = service_account.Credentials.from_service_account_info(
            certificate_, scopes=scopes_)
        service = googleapiclient.discovery.build(
            serviceName=service_name_,
            version=version_,
            credentials=credentials,
            cache_discovery=False)
        return service

    def get_access_token(self):

        logging.error("get_access_token() from API base class not currently setup.")