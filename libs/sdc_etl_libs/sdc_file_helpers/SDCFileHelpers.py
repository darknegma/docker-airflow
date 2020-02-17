
import csv
import logging
import os


class SDCFileHelpers:

    @staticmethod
    def convert_file_to_flattened_dict(file_object_, file_type_='csv',
                                       delimiter_=',', column_header_list_=None):
        """
        Converts a file object to a list of flattened dictionaries.

        :param file_object_: File object that contains the lines of data to
            be processed.
        :param file_type_: File type of object. Default = 'csv'.
        :param delimiter_: File delimiter of object. Default = ','.
        :param column_header_list_: List of columns headers. Default = None.
        :return: List of flattened dictionaries.
        """

        output = []

        if file_type_.lower() in ['txt', 'csv']:
            try:
                if column_header_list_:
                    reader = csv.DictReader(file_object_, delimiter=delimiter_,
                                            fieldnames=column_header_list_)
                else:
                    reader =csv.DictReader(file_object_, delimiter=delimiter_)

            except Exception as e:
                logging.error(e)
                logging.error(f"Failed flattening file.")

        else:
            raise Exception(f"Error flattening file to dict. '{file_type_}' "
                            f"currently not support in SDCFileHelpers")

        for line in reader:
            output.append(dict(line))

        return output

    @staticmethod
    def get_file_path(type_, path_):
        """
        Returns absolute file path for file type and relative path provided.
        Function uses this file a point of reference to determine the
        right absolute path.
        :param type_: Type of file to be retrieved. Options:
            'schema': Returns schema from schema directory.
            'sql': Returns sql from sql directory.
        :param path_: Path to file from the desired type_.
        :return: Path to file as string.
        """

        if type_ == 'schema':
            file_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), '..', 'schemas',
                path_
            )

        elif type_ == 'sql':
            file_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), '..', 'sql',
                path_
            )
 
        elif type_ == 'metadata':
            file_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), '..', 'metadata',
                path_
            )

        else:
            raise Exception("File type for get_file_path not supported.")

        if not os.path.exists(file_path):
            raise Exception("File provided does not exist.")

        return file_path

    @staticmethod
    def compare_json_schema(obj1,obj2,format="json"):
        """
        :params obj1/obj2 two objects to compare
        :returns 0 for no difference detected, 1 - difference found
        """
        if len(obj1)!=len(obj2):
            logging.info(f"NUMBER OF KEYS DIDN'T MATCH!!")
            return True

        if format=="json":
            for key, value in obj1.items():
                if key not in obj2:
                    logging.info(f"KEY '{key}' NOT FOUND!!")
                    return True
                elif key in obj2 and obj1[key]==obj2[key]:
                    logging.info(f"KEY '{key}' MATCH FOUND!")
                    continue
                elif isinstance(obj1[key], dict) and isinstance(obj2[key], dict):
                    if SDCFileHelpers.compare_json_schema(obj1[key],obj2[key]) == 1:
                        return True
                elif isinstance(obj1[key], list) and isinstance(obj2[key], list):
                    if SDCFileHelpers.compare_json_schema(obj1[key],obj2[key],"list") == 1:
                        return True
                elif type(obj1[key])!=type(obj2[key]):
                    logging.info(f"KEY '{key}' DIDN'T MATCH!!")
                    return True
                elif value != obj2[key]:
                    logging.info(f"KEY '{key}' DIDN'T MATCH!!")
                    return True
        elif format=="list":
            for key, val in enumerate(obj1):
                if val not in obj2:
                    logging.info(f"KEY '{key}' NOT FOUND!!")
                    return True
            for key, val in enumerate(obj2):
                if val not in obj1:
                    logging.info(f"KEY '{key}' NOT FOUND!!")
                    return True
                # this could be expanded with additional cases to iterated deeper through lists of dictionaries
        return False