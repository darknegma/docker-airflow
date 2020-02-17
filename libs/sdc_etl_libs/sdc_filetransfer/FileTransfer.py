
import logging
from abc import ABCMeta, abstractmethod

class FileTransfer(metaclass=ABCMeta):
    connection = None

    def __init__(self):
        logging.info("Base constructor.")

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def get_obj_list(self):
        pass

    @abstractmethod
    def get_obj_stats(self):
        pass

    @abstractmethod
    def close_connection(self):
        pass
