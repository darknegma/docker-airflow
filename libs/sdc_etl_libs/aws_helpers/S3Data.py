import boto3
from botocore.exceptions import ClientError
import gzip
import io
import os
import csv
import re

class S3Data(object):

    def __init__(self, bucket_name_, prefix_, file_, df_schema_, compression_type_,
                 check_headers_, file_type_, access_key_=None, secret_key_=None,
                 region_='us-east-2', decode_='utf-8'):
        """
                       Writes a compressed s3 file to snowflake

                       :param bucket_name_: S3 bucket
                       :param prefix_: S3 prefix (i.e., "directory path" of bucket)
                       :param file_: S3 file / obj
                       :param df_schema_: Schema as defined by the json file for this object
                       :param check_headers_: True or False to check for headers
                       :param file_type_: S3 object file type (if applicable). Currently,
                           this function only supports 'csv'. and 'tsv'
                       :param compression_type: type of compression being used.
                       :param access_key_: AWS Access Key for S3 bucket (if applicable)
                       :param secret_key_: AWS Secret Key for S3 bucket (if applicable)
                       :param region_: AWS region (Default = 'us-east-2')
                       :param decode_: Character decoding of object

                       :return: List of Data Dictionaries
        """

        # File connection properties
        self.file_name = file_
        self.df_schema = df_schema_
        self.region = region_
        self.decode = decode_
        self.bucket_name = bucket_name_
        self.prefix = prefix_
        self.file_type = file_type_
        self.compression_type = compression_type_
        self.access_key = access_key_
        self.secret_key = secret_key_

        # file processing properties
        self.check_headers = check_headers_
        self.current_row = 1
        self.lines = None
        self.field_names = []      #for snowflake and dataframe processing
        self.field_names_file =[]  #for column header check
        self.table = ''
        self.schema = ''
        self.file_row_count = 0

        if self.file_type == "csv":
            self.delimiter = b','
        elif self.file_type == "tsv":
            self.delimiter = b'\t'
        else:
            raise Exception(f"File type: {self.file_type} not supported.")

    def __read_data(self, s3_data_):
        if self.compression_type == "gz":
            file_handle = io.BytesIO(s3_data_)
            self.lines = gzip.open(file_handle)
        elif isinstance(self.compression_type,type(None)):
            self.lines = s3_data_.decode('utf-8').split()
        else:
            raise Exception(f"Compression type: {self.compression_type} not supported.")

    def load_data(self,skip_schema=False):
        """
          Makes the connection, opens the file as bytes
          calls the load schema
          calls the load file row count
          return: None
           """
        client = boto3.client('s3', region_name=self.region,
                              aws_access_key_id=self.access_key,
                              aws_secret_access_key=self.secret_key)

        obj = client.get_object(Bucket=self.bucket_name, Key=self.prefix + self.file_name)
        s3_data = obj['Body'].read()
        self.__read_data(s3_data)
        if skip_schema is False:
            self.__load_schema()

    def __load_schema(self):
        """
          Private Function
          Loads the schema file into a dictionary
          Retrieves table and schema values
          return: None
           """

        # retrieve some data from our json
        # get our field names
        self.field_names.clear()
        for f in self.df_schema['fields']:
            self.field_names.append(f['name'])
            self.field_names_file.append(f['name_in_file'])

        # retrieve what table and schema we are using from the json and pass into the dataframe
        self.table = self.df_schema['data_sink']['table_name']
        self.schema = self.df_schema['data_sink']['schema']

    def __get_file_size(self):
        """
        Private Function
        gets the row count of the file

        return: None sets the self.file_row_count value
        """
        row_count = 1
        has_column_header = False

        for line in self.lines:
            newline = line.rstrip().lstrip().split(self.delimiter)

            if self.current_row == 1:  # check once for column header
                if self.check_headers:
                    if str(newline[0].decode(self.decode)).upper() in self.field_names_file:
                        has_column_header = True

            if has_column_header == False:
                row_count += 1
            else:
                has_column_header = False

        self.file_row_count = row_count

    def get_file_record_count(self):
        if self.file_row_count > 0:
            return self.file_row_count

        self.__get_file_size()
        self.lines.close()
        self.load_data()
        self.current_row = 1
        return self.file_row_count

    def get_records(self, row_to_start_: int, chunk_size_: int, file_id_=None):
        """
         Loads a set of data into a list of dictionary objects. Keeps track of the row number pointer
        :param row_to_start_: The row number to start processing. Note: header records do not count as a row
        :param chunk_size_: the number of records to process
        :param file_id_: Required parameter when adding the column FILE_ID to the data schema and using the ProcessLogger.
        return: List of Data Dictionaries
        """
        data = []
        has_column_header = False

        row_to_end = chunk_size_ + row_to_start_

        if row_to_start_ >= self.current_row:
            pass
        else:
            self.lines.close()
            self.load_data()
            self.current_row = 1

        for line in self.lines:
            if self.current_row >= row_to_start_:
                data_dict = {}
                newline = line.rstrip().lstrip().split(self.delimiter)

                if self.current_row == 1:  # check once for column header
                    if self.check_headers:
                        if str(newline[0].decode(self.decode)).upper() in self.field_names_file:
                            has_column_header = True

                if not has_column_header:
                    column_number = 0
                    for fields in self.field_names:
                        if fields == "FILE_ID":
                            if not isinstance(file_id_, type(None)):
                                data_dict[fields] = file_id_
                            else:
                                raise Exception("Missing file id field")
                        else:
                            data_dict[fields] = str(newline[column_number].decode(self.decode))

                        column_number += 1

                if len(data_dict) > 0:
                    data.append(data_dict)

            if not has_column_header:
                self.current_row += 1

            has_column_header = False

            if self.current_row >= row_to_end:
                break

        return data

    @staticmethod
    def iterate_on_s3_response(response_: dict, bucket_name_: str,
                               prefix_: str, files_: list, give_full_path_):
        """
        Iterate over an S3 List Objects result and adds object file/object
        names to list.
        :param response_: Response from  List Objects func.
        :param bucket_name_: Name of S3 bucket that was searched.
        :param prefix_: Prefix used to find files.
        :param files_: List append S3 URLs to.
        :return: None
        """

        for item in response_["Contents"]:
            if prefix_ in item["Key"]:
                if give_full_path_:
                    files_.append("s3://" + bucket_name_ + "/" + item["Key"])
                else:
                    files_.append(os.path.basename(item["Key"]))

    @staticmethod
    def get_file_list_s3(bucket_name_, prefix_, access_key_=None,
                         secret_key_=None, region_='us-east-2',
                         file_prefix_: str = None, file_suffix_: str = None,
                         file_regex_: str = None, give_full_path_=False):
        """
        Creates a list of items in an S3 bucket.
        :param bucket_name_: Name of S3 bucket to search
        :param prefix_: Prefix used to find files.
        :param access_key_: AWS Access Key for S3 bucket (if applicable)
        :param secret_key_: AWS Secret Key for S3 bucket (if applicable)
        :param region_: AWS region (Default = 'us-east-2')
        :param file_prefix_: If used, function will return files that start
            with this (case-sensitive). Can be used in tandem with file_suffix_
        :param file_suffix_: If used, function will return files that end
            with this (case-sensitive). Can be used in tandem with file_prefix_
        :param file_regex_: If used, will return all files that match this
            regex pattern. file_prefix_ & file_suffix_ will be ignored.
        :param give_full_path_: If False, only file name will be returned. If
            true, full path & file name will be returned.
        :return: List of S3 file/object names as strings
        """

        client = boto3.client('s3', region_name=region_,
                              aws_access_key_id=access_key_,
                              aws_secret_access_key=secret_key_)

        response = client.list_objects_v2(Bucket=bucket_name_, Prefix=prefix_)

        all_files = []

        if "Contents" in response:
            S3Data.iterate_on_s3_response(response, bucket_name_,
                                          prefix_, all_files, give_full_path_)
            while response["IsTruncated"]:
                print(response["NextContinuationToken"])
                response = client.list_objects_v2(
                    Bucket=bucket_name_, Prefix=prefix_,
                    ContinuationToken=response["NextContinuationToken"])
                S3Data.iterate_on_s3_response(response, bucket_name_,
                                              prefix_, all_files, give_full_path_)

            if file_regex_ or file_prefix_ or file_suffix_:
                pattern = file_regex_ if file_regex_ else \
                    f"{file_prefix_ if file_prefix_ else ''}.*{file_suffix_ if file_suffix_ else ''}"

                files = [x for x in all_files if re.search(pattern, x)]

            else:
                files = all_files

        return files

    @staticmethod
    def s3_obj_to_dict(bucket_name_, prefix_, file_, file_type_='csv',
                       access_key_=None, secret_key_=None, region_='us-east-2',
                       decode_='utf-8'):
        """
        Converts an S3 object to a list of flattened dictionary records.

        :param bucket_name_: S3 bucket
        :param prefix_: S3 prefix (i.e., "directory path" of bucket)
        :param file_: S3 file / obj
        :param file_type_: S3 object file type (if applicable). Currently,
            this function only supports 'csv'. No other file types have been
            tested.
        :param access_key_: AWS Access Key for S3 bucket (if applicable)
        :param secret_key_: AWS Secret Key for S3 bucket (if applicable)
        :param region_: AWS region (Default = 'us-east-2')
        :param decode_: Character decoding of object
        :return: List of flattened dictionaries.
        """

        if not file_type_:
            raise Exception("Need to pass a file_type_.")

        client = boto3.client('s3', region_name=region_,
                              aws_access_key_id=access_key_,
                              aws_secret_access_key=secret_key_)

        obj = client.get_object(Bucket=bucket_name_, Key=prefix_ + file_)

        lines = obj['Body'].read().decode(decode_).split()
        data = []

        try:
            if file_type_ == 'csv':
                reader = csv.DictReader(lines)
                for row in reader:
                    data.append(dict(row))
            else:
                raise Exception(f"{file_type_} not currently supported for S3 obj conversion.")

            return data

        except Exception as e:
            raise Exception(f"There was an issue converting {file_} to a "
                            f"list of flattened dictionaries. {e}")

    def upload_file(self,object_name_=None):
        """Upload a file to an S3 bucket

        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if object_name_ is None:
            object_name_ = self.prefix+"/"+self.file_name
        else:
            object_name_ = self.prefix+"/"+object_name_

        # Upload the file
        client = boto3.client('s3', region_name=self.region,
                                aws_access_key_id=self.access_key,
                                aws_secret_access_key=self.secret_key)
        try:
            response = client.upload_file(Filename=self.file_name, Bucket=self.bucket_name, Key=object_name_)
        except ClientError as e:
            print(f"There was an error {e} while uploading {self.file_name} to S3")
            return False
        target_path=f"https://{self.bucket_name}.s3.{self.region}.amazonaws.com/{object_name_}"
        return target_path
    
    def save_to_s3(self, data_string_):
        """
        Saves data as a file in S3 bucket
        :param data_string_: data to be saved
        return: target_path
        """
        encoded_data=data_string_.encode(self.decode)
        s3_path=self.prefix+"/"+self.file_name

        resource=boto3.resource('s3', region_name=self.region,
                                aws_access_key_id=self.access_key,
                                aws_secret_access_key=self.secret_key)
        try:
            response=resource.Bucket(self.bucket_name).put_object(Key=s3_path, Body=encoded_data)
        except ClientError as e:
            print(f"There was an error {e} while uploading {self.file_name} to S3")
            return False
        target_path=f"https://{self.bucket_name}.s3.{self.region}.amazonaws.com/{s3_path}"
        return target_path

