from abc import ABCMeta, abstractmethod


class SDCDataExchangeEndpoint(metaclass=ABCMeta):

    def create_exchange_endpoint(self):
        raise Exception("Do not use base class implementation of create_exchange_endpoint.")

    def get_data_as_file_object(self):
        raise Exception("Do not use base class implementation of get_data_as_file_object.")

    def write_file(self, file_name_, file_object_):
        raise Exception("Do not use base class implementation of write_file.")

    def write_dataframe(self, sdcdf_, **kwargs):
        raise Exception("Do not use base class implementation of write_dataframe.")


