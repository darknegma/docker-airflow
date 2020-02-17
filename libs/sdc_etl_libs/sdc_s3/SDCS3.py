
import io
import logging
import re
import boto3
import os
from botocore.exceptions import ClientError

class SDCS3():

    def __init__(self):

        self.access_key = None
        self.secret_key = None
        self.region = None
        self.client = None

    def connect(self, access_key_=None, secret_key_=None, region_='us-east-2'):
        """
        Creates S3 client and returns handle to instance's 'client' attribute.
        :param access_key_: AWS Access Key. Default None.
        :param secret_key_: AWS Secret Key. Default None.
        :param region_: AWS Region. Default 'us-east-2'.
        :return: None.
        """

        self.region = region_
        self.client = boto3.client(
            's3', region_name=self.region,
            aws_access_key_id=access_key_,
            aws_secret_access_key=secret_key_)

    def get_obj_list(self, bucket_name_, prefix_, obj_regex_=None,
                     give_full_path_=False):
        """
        Creates a list of items in an S3 bucket.
        :param bucket_name_: Name of S3 bucket to search
        :param prefix_: Prefix used to find files.
        :param obj_regex_: If used, will return all objects that match this
            regex pattern. Default None.
        :param give_full_path_: If False, only file name will be returned. If
            true, full path & file name will be returned. Default False.
        :return: List of S3 file/object names as strings.
        """

        def iterate_on_s3_response(response_, files_, **kwargs):
            for item in response_["Contents"]:
                if kwargs["prefix_"] in item["Key"]:
                    if kwargs["give_full_path_"]:
                        files_.append("s3://" + bucket_name_ + "/" + item["Key"])
                    else:
                        files_.append(os.path.basename(item["Key"]))

        available_objects = []
        object_results = []

        response = self.client.list_objects_v2(Bucket=bucket_name_, Prefix=prefix_)

        if "Contents" in response:
            iterate_on_s3_response(response_=response,
                                   files_=available_objects,
                                   bucket_name_=bucket_name_,
                                   prefix_=prefix_,
                                   give_full_path_=give_full_path_)
            while response["IsTruncated"]:
                logging.info(response["NextContinuationToken"])
                response = self.client.list_objects_v2(
                    Bucket=bucket_name_, Prefix=prefix_,
                    ContinuationToken=response["NextContinuationToken"])
                iterate_on_s3_response(response_=response,
                                       files_=available_objects,
                                       bucket_name_=bucket_name_,
                                       prefix_=prefix_,
                                       give_full_path_=give_full_path_)

            if obj_regex_:
                object_results = \
                    [x for x in available_objects if re.search(obj_regex_, x)]

            else:
                object_results = available_objects

        return object_results

    def get_obj_stats(self, bucket_name_, prefix_, obj_name_):
        """
        Returns a dictionary of an objects stats, including size and last
        modified date.
        :param bucket_name_: Bucket name.
        :param prefix_: File prefix (i.e. S3 folder path)
        :param obj_name_: Name of object.
        :return: Dictionary of various object stats.
        """

        stats = {}

        try:
            obj_header = self.client.head_object(
                Bucket=bucket_name_, Key=prefix_ + obj_name_)

            stats["size_bytes"] = obj_header["ContentLength"]
            stats["size_mb"] = obj_header["ContentLength"] / 1048576
            stats["last_modified"] = obj_header["LastModified"]

        except ClientError as e:
            logging.info(
                f"There was an error retrieving stats for {obj_name_}. {e} ")

        return stats

    def get_file_as_file_object(self, bucket_name_, prefix_, file_name_, decode_):
        """
        Creates a file object from a given S3 file.
        :param bucket_name_: Bucket name.
        :param prefix_: File prefix (i.e. S3 folder path).
        :param file_name_: Name to write object as in S3.
        :param decode_: the character encoding of the string. ex: utf-8
        :return: file like object: StringIO.
        """

        file_obj = self.client.get_object(
            Bucket=bucket_name_,
            Key=prefix_ + file_name_)

        s_buf = io.StringIO(file_obj["Body"].read().decode(decode_))
        return s_buf

    def write_file(self, bucket_name_, prefix_, file_name_, file_obj_):
        """
        Takes a file object and writes out contents to file on S3.
        :param bucket_name_: Bucket name.
        :param prefix_: File prefix (i.e. S3 folder path).
        :param file_name_: Name to write object as in S3.
        :param file_obj_: File as an object to write to S3.
        :return: None.
        """

        try:
            self.client.put_object(
                Body=file_obj_.read(),
                Bucket=bucket_name_,
                Key=prefix_ + file_name_)

            stats = self.get_obj_stats(bucket_name_, prefix_, file_name_)

            result = f'{round(stats["size_mb"], 5):,} MB'
            logging.info(result)

            return result

        except ClientError as e:
            logging.info(f"There was an error {e} while uploading {file_name_} "
                         f"to S3")

    def write_dataframe(self, df_, bucket_name_, prefix_, file_type_,
                        file_name_, **kwargs):
        """
        Writes a dataframe out to S3 bucket.
        :param df_: SDCDataframe object.
        :param bucket_name_: Name of S3 bucket to search
        :param prefix_: Prefix used to find files.
        :param file_type_: Type of file.
        :param file_name_: Name of the file.
        :param kwargs : Key word arguments that correspond to the file type
            being written out. ex: type: csv args: file_delimiter_, headers_
        :return: Result log of file transfer as string.
        """

        rows, cols = df_.df.shape

        logging.info(f"Writing {file_name_} to S3 {bucket_name_}. "
                     f"{cols} columns, {rows} records.")

        try:
            fileobj = df_.get_as_file_obj(file_type_, **kwargs)

            self.client.upload_fileobj(
                Fileobj=io.BytesIO(fileobj.getvalue().encode()),
                Bucket=bucket_name_,
                Key=prefix_ + file_name_)

            stats = self.get_obj_stats(bucket_name_, prefix_, file_name_)
            result = f'{round(stats["size_mb"], 5):,} MB'

            return result

        except ClientError as e:
            logging.info(
                f"There was an error while uploading {file_name_} to S3. {e}")
