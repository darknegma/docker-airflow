
import logging
from sdc_etl_libs.sdc_exchange_endpoints.file_endpoints.SDCSFTPEndpoint import SDCSFTPEndpoint
from sdc_etl_libs.sdc_exchange_endpoints.file_endpoints.SDCS3Endpoint import SDCS3Endpoint
from sdc_etl_libs.sdc_exchange_endpoints.database_endpoints.SDCSnowflakeEndpoint import SDCSnowflakeEndpoint
from sdc_etl_libs.sdc_exchange_endpoints.database_endpoints.SDCPostgresEndpoint import SDCPostgresEndpoint

available_endpoints = {
        'sftp': SDCSFTPEndpoint(),
        's3': SDCS3Endpoint(),
        'snowflake': SDCSnowflakeEndpoint(),
        'postgres': SDCPostgresEndpoint()
}

class SDCDataExchangeEndpointFactory(object):

    @staticmethod
    def get_endpoint(data_schema_, endpoint_schema_):
        """
        Factory for returning the proper SDCDataExcbangeEndpoint class.
        :param data_schema_: JSON of data schema.
        :param endpoint_schema_: JSON of endpoint schema.
        :return: SDCDataExchange class.
        """

        endpoint_type_ = endpoint_schema_["type"]

        if endpoint_type_ in available_endpoints.keys():
            endpoint = available_endpoints[endpoint_type_]
            endpoint.create_exchange_endpoint(data_schema_, endpoint_schema_)
            return endpoint
        else:
            logging.exception(f"{endpoint_type_} is not a valid endpoint option.")
