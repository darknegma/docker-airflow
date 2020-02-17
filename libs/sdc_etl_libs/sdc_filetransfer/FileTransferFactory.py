
import logging
from sdc_etl_libs.sdc_filetransfer.SFTPFileTransfer import SFTP
from sdc_etl_libs.sdc_filetransfer.SSHFileTransfer import SSH


available_ftps = {
        'sftp': SFTP()
        }


class FileTransferFactory(object):

    @staticmethod
    def get_ftp(ftp_name_):
        if ftp_name_ in available_ftps.keys():
            return available_ftps[ftp_name_]
        else:
            logging.exception(f"{ftp_name_} is not a valid ftp option.")
