
import errno
import logging
import os
import re
import stat
import paramiko
from io import StringIO
from sdc_etl_libs.sdc_filetransfer.FileTransfer import FileTransfer


class SFTP(FileTransfer):

    def __init__(self):

        self.host = None
        self.username = None
        self.port = None
        self.transport = None
        self.client = None
        self.rsa_key = None

    def connect(self, host_, username_, port_, password_=None, rsa_key_=None):
        """
        Connects to SFTP site and returns a handle to self.client.
        :param host_: SFTP hostname.
        :param username_: SFTP username.
        :param password_: SFTP password. Optional.
        :param rsa_key_: RSA Public or Private Key. Optional.
        :param port_: Int. SFTP port.
        :return: None.
        """

        self.host = host_
        self.username = username_
        self.port = int(port_)
        self.transport = paramiko.Transport((self.host, self.port))

        if password_:
            self.transport.connect(username=self.username, password=password_)
        elif rsa_key_:
            pkey = paramiko.RSAKey(file_obj=StringIO(rsa_key_))
            self.transport.start_client()
            self.transport.auth_publickey(username_, pkey)

        self.client = paramiko.SFTPClient.from_transport(self.transport)

    def check_if_path_exists(self, path_):
        """
        Checks to see if path exists on SFTP site.
        :param path_: Path to object (Can be file for directory).
        :return: Boolean.
        """

        try:
            self.client.stat(path_)
            return True
        except IOError as e:
            if e.errno == errno.ENOENT:
                return False

    def get_obj_list(self, path_=None, obj_regex_=None, give_full_path_=False,
                     include_dirs_=True):
        """
        Returns a list of objects in a path on an SFTP site.
        :param path_: Path to return object names from.
            Default '' (i.e. current directory).
        :param obj_regex_: Regex expression to search/return object
            names by. Default None.
        :param give_full_path_: If False, returns only object names. If True,
            If True, returns full path (from path_) of object. Default False.
        :param include_dirs_: If True, will remove directories
            from returned list results. If False, directories will be included.
        :return: List of SFTP object names as strings. Default False.
        """

        # If no path specified, set path to connection's starting directory.
        path = os.path.join(path_, '') if path_ else ''

        if self.check_if_path_exists(path):

            available_objects = []
            object_results = []

            if include_dirs_:
                available_objects = self.client.listdir(path)
            else:
                for obj in self.client.listdir(path):
                    full_path = os.path.join(path, obj)
                    if not stat.S_ISDIR(self.get_obj_stats(full_path).st_mode):
                        available_objects.append(obj)

            if obj_regex_:
                available_objects = \
                    [x for x in available_objects if re.search(obj_regex_, x)]

            for obj in available_objects:
                if give_full_path_:
                    object_results.append(os.path.join(path, obj))
                else:
                    object_results.append(obj)

            return object_results

        else:
            raise Exception(f"Path {path} does not exist on site.")

    def get_obj_stats(self, obj_path_):
        """
        Returns information about object on SFTP site.
        :param obj_path_: Path of object.
        :return: SFTPAttributes object that holds attributes returned by the
        site, which can vary. Attributes supported are st_mode, st_size,
            st_uid, st_gid, st_atime, st_mtime.
        """

        try:
            return self.client.stat(obj_path_)

        except FileNotFoundError:
            logging.warning(f"{obj_path_} was not found on the SFTP site.")

        except Exception as e:
            logging.warning(f"An error occurred. {e}")

    def close_connection(self):
        """
        Closes the SFTP connection.
        :return: None.
        """

        try:
            logging.info("Closing connection...")
            self.transport.close()
            logging.info("Connection closed.")

        except Exception as e:
            logging.warning(f"There was an issue closing the connection: {e}")

    def get_file_as_file_object(self, file_path_):
        """
        Returns a file as a file object.
        :param file_path_: Path of file on SFTP site.
        :return: File object, type of "paramiko.sftp_file.SFTPFile".
        """

        file_obj = self.client.file(file_path_, 'r')
        file_obj.seek(0)

        return file_obj

    def write_file(self, path_, file_name_, file_obj_):
        """
        Writes a file object to SFTP site.
        :param path_: Path to write object to.
        :param file_name_: Name to write object as.
        :param file_obj_: File as an object to write out.
        :return: None.
        """
        file_destination = os.path.join(path_, file_name_)

        try:
            with self.client.open(file_destination, "w") as f:
                f.write(file_obj_.getvalue())

            stats = self.get_obj_stats(file_destination)

            result = f"{round(stats.st_size / 1000000, 5):,} MB"
            logging.info(result)

            return result

        except Exception as e:
            logging.exception(
                f"There was an issue writing this dataframe to the SFTP site: "
                f"{str(e)}")

    def write_dataframe(self, df_, path_, file_type_, file_name_, **kwargs):
        """
        Writes a SDCDataframe's dataframe out to SFTP site as a file.
        :param df_: SDCDataframe object.
        :param file_type_: Type of file.
        :param path_: SFTP path to write dataframe to.
        :param file_name_: Name of the file.
        :param kwargs : Key word arguments that correspond to the filetype
            being written. ex: type: csv args: file_delimiter_, headers_,
            column_order_
        :return: Result log of file transfer as string.
        """

        rows, cols = df_.df.shape

        logging.info(f"Writing {file_name_} to SFTP {self.host}. "
                     f"{cols} columns, {rows} records.")
        file_destination = os.path.join(path_, file_name_)

        try:
            with self.client.open(file_destination, "w") as f:
                f.write(df_.get_as_file_obj(
                    file_type_, **kwargs).getvalue())

            stats = self.get_obj_stats(file_destination)

            result = f"{round(stats.st_size / 1000000, 5):,} MB"

            return result

        except Exception as e:
            self.client.remove(file_destination)
            logging.exception(
                f"There was an issue writing this dataframe to the SFTP site: "
                f"{str(e)}")
