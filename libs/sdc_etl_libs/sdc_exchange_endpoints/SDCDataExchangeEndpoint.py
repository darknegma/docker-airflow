from abc import ABCMeta, abstractmethod


class SDCDataExchangeEndpoint(metaclass=ABCMeta):

    @abstractmethod
    def write_data(self):
        pass
    @abstractmethod
    def get_data(self):
        pass
