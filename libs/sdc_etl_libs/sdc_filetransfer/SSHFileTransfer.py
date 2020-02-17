

import logging
from sdc_etl_libs.sdc_filetransfer.FileTransfer import FileTransfer


class SSH(FileTransfer):
    """
    Placeholder for SSH file transfers if we have the need for this
    """

    def __init__(self):

        raise Exception("SSH file transfer is not an option at this time.")

    def connect(self):
        pass

    def get_obj_list(self):
        pass

    def get_obj_stats(self):
        pass

    def close_connection(self):
        pass
