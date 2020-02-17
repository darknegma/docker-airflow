
import copy
import logging
import json
from sdc_etl_libs.database_helpers.SnowflakeDatabase import SnowflakeDatabase
from sdc_etl_libs.sdc_data_exchange.SDCDataExchangeEnums import FileExchangeTypes
from sdc_etl_libs.sdc_data_schema.SDCDataSchemaEnums import *
from sdc_etl_libs.database_helpers.Database import SDCDBTypes
from sdc_etl_libs.sdc_file_helpers.SDCFileHelpers import SDCFileHelpers

class SDCDataSchema:

    #TODO: add function to validate entoire schema to make creating schemas easier.

    @staticmethod
    def parse_endpoint_data(schema_data_json_, endpoint_tag_):
        """
        Parses the endpoint portion of a data schema.
        :param schema_data_json_: Schema to parse (JSON format)
        :param endpoint_tag_: Tag of data sink/source.
        :return: Dictionary of data_sink record.
        """
        data_endpoints = []
        sources = copy.deepcopy(schema_data_json_["data_source"])
        sinks = copy.deepcopy(schema_data_json_["data_sink"])
        if isinstance(sources, list):
            data_endpoints.extend(sources)
        elif isinstance(sources, dict):
            data_endpoints.append(sources)
        else:
            raise Exception("Malformed data_source")

        if isinstance(sinks, list):
            data_endpoints.extend(sinks)
        elif isinstance(sources, dict):
            data_endpoints.append(sinks)
        else:
            raise Exception("Malformed data_sink")


        if not isinstance(data_endpoints, list):
            raise Exception("A list was not passed to parse the endpoint sink/source data.")

        result = []
        for rec in data_endpoints:
            if rec["tag"] == endpoint_tag_:
                result.append(rec)

        if not result:
            raise Exception(f"Failed processing schema data. No "
                            f"records round for tag: {endpoint_tag_}")
        elif len(result) > 1:
            raise Exception(f"Failed parsing schema data. {len(result)} "
                            f"records found for tag: {endpoint_tag_}")
        else:
            record = result[0]
            try:
                if record["endpoint_type"] == 'sink':
                    SDCDataSchema.validate_sink(record)
                elif record["endpoint_type"] == 'source':
                    SDCDataSchema.validate_source(record)
                if "credentials" in record:
                    if record["credentials"]:
                        SDCDataSchema.validate_credentials(record)
                logging.info("Schema passed validation!")
            except Exception as e:
                raise Exception(f"Schema failed validation {e}")

        return record

    @staticmethod
    def validate_sink(sink_json_):
        """
        Validates sink information of data schema.
        :param sink_json_: Sink JSON record.
        :return: None
        """

        endpoint_type = sink_json_["type"]
        if endpoint_type == "s3":
            mandatory_items = S3SinkInfoMandatory.__members__
            optional_items = S3SinkInfoOptional.__members__
            data_type_items = S3InfoDataTypes.__members__
        elif endpoint_type == 'sftp':
            mandatory_items = SFTPSinkInfoMandatory.__members__
            optional_items = SFTPSinkInfoOptional.__members__
            data_type_items = SFTPInfoDataTypes.__members__
        elif endpoint_type == 'snowflake':
            mandatory_items = SnowflakeSinkInfoMandatory.__members__
            optional_items = SnowflakeSinkInfoOptional.__members__
            data_type_items = SnowflakeInfoDataTypes.__members__

        validation_errors = []
        ignored_items = []

        # If a mandatory item is not in schema, add to error log:
        for item in mandatory_items:
            if item not in sink_json_:
                validation_errors.append(f'"{item}" is missing and is mandatory.')
        # If an item in schema needs to have a specific data type and is not that type, add to error log:
        for key, value in sink_json_.items():
            if key in data_type_items:
                if type(value) != data_type_items[key].value:
                    validation_errors.append(
                        f'"{key}" is not of type {data_type_items[key].value}.')
        # If an optional item is not in the schema, add item to schema and set the value to None:
        for item in optional_items:
            if item not in sink_json_:
                sink_json_[item] = None
        # If an item in the schema is not mandatory or optional, add to ignore log:
        for item in sink_json_:
            if item not in mandatory_items and item not in optional_items:
                ignored_items.append(item)

        # If there were any errors, raise exception as can't proceed:
        if validation_errors:
            raise Exception(
                f"ERROR: Schema validation failed due to the following "
                f"reasons: {validation_errors} ")
        # If there were any ignored items, let user know so they know it does nothing:
        if ignored_items:
            logging.error(f"Note: The following options are being ignored "
                         f"{ignored_items} for {endpoint_type} sink.")

        # Depending on endpoint type, run additional functions on schema:
        if endpoint_type in FileExchangeTypes.__members__:
            SDCDataSchema.validate_file_info(sink_json_, "sink")
        elif endpoint_type in SDCDBTypes.__members__:
            pass

        logging.info("SCHEMA VALIDATION: Done validating sink information.")

    @staticmethod
    def validate_source(source_json_):
        """
        Validates source information of data schema.
        :param source_json_: Source JSON record.
        :return: None
        """

        endpoint_type = source_json_["type"]
        if endpoint_type == "s3":
            mandatory_items = S3SourceInfoMandatory.__members__
            optional_items = S3SourceInfoOptional.__members__
            data_type_items = S3InfoDataTypes.__members__
        elif endpoint_type == 'sftp':
            mandatory_items = SFTPSourceInfoMandatory.__members__
            optional_items = SFTPSourceInfoOptional.__members__
            data_type_items = SFTPInfoDataTypes.__members__
        elif endpoint_type == 'snowflake':
            mandatory_items = SnowflakeSourceInfoMandatory.__members__
            optional_items = SnowflakeSourceInfoOptional.__members__
            data_type_items = SnowflakeInfoDataTypes.__members__

        validation_errors = []
        ignored_items = []

        for item in mandatory_items:
            if item not in source_json_:
                validation_errors.append(f'"{item}" is missing and is mandatory.')
        for key, value in source_json_.items():
            if key in data_type_items:
                if type(value) != data_type_items[key].value:
                    validation_errors.append(
                        f'"{key}" is not of type {data_type_items[key].value}.')
        for item in optional_items:
            if item not in source_json_:
                source_json_[item] = None
        for item in source_json_:
            if item not in mandatory_items and item not in optional_items:
                ignored_items.append(item)

        if validation_errors:
            raise Exception(f"ERROR: Schema validation failed due to the following "
                            f"reasons: {validation_errors} ")
        if ignored_items:
            logging.error(f"Note: The following options are being ignored "
                          f"{ignored_items} for {endpoint_type} source.")

        if endpoint_type in FileExchangeTypes.__members__:
            SDCDataSchema.validate_file_info(source_json_, "source")
        elif endpoint_type in SDCDBTypes.__members__:
            # DB sepcific source validation here - including query file location
            pass

        logging.info("SCHEMA VALIDATION: Done validating source information.")

    @staticmethod
    def validate_file_info(endpoint_json_, endpoint_type_):
        """
        Validates the optional "file_info" section of schema source/sink data.
        :param endpoint_json_: Data sink/source JSON record.
        :param endpoint_type_: Type of endpoint being vaidated (sink/source)
        :return: None.
        """

        endpoint_type = endpoint_json_["type"]
        if endpoint_type in FileExchangeTypes.__members__:

            file_info_data = endpoint_json_["file_info"]
            file_type = file_info_data["type"]
            data_type_items = FileInfoDataTypes.__members__
            endpoint_json_["file_info"]["type"]

            if endpoint_type_ == "sink":
                if file_type == 'file':
                    mandatory_items = FileInfoFileSinkMandatory.__members__
                    optional_items = FileInfoFileSinkOptional.__members__
                elif file_type== 'csv':
                    mandatory_items = FileInfoCSVSinkMandatory.__members__
                    optional_items = FileInfoCSVSinkOptional.__members__
                elif file_type == 'parquet':
                    mandatory_items = FileInfoParquetSinkMandatory.__members__
                    optional_items = FileInfoParquetSinkOptional.__members__
                else:
                    raise Exception(f"File type of {file_type} not supported.")

            elif endpoint_type_ == "source":
                if file_type == 'file':
                    mandatory_items = FileInfoFileSourceMandatory.__members__
                    optional_items = FileInfoFileSourceOptional.__members__
                elif file_type == 'csv':
                    mandatory_items = FileInfoCSVSourceMandatory.__members__
                    optional_items = FileInfoCSVSourceOptional.__members__
                elif file_type == 'parquet':
                    mandatory_items = FileInfoParquetSourceMandatory.__members__
                    optional_items = FileInfoParquetSourceOptional.__members__
                else:
                    raise Exception(f"File type of {file_type} not supported.")

            ignored_items = []
            validation_errors = []

            logging.info("Validating schema file info options...")
            for item in mandatory_items:
                if item not in file_info_data:
                    validation_errors.append(f"{item} is missing and is mandatory.")
            for key, value in file_info_data.items():
                if key in data_type_items:
                    if type(value) != data_type_items[key].value:
                        validation_errors.append(
                            f'"{key}" is not of type {data_type_items[key].value}.')
            for item in optional_items:
                if item not in file_info_data:
                    file_info_data[item] = None
            for item in file_info_data:
                if item not in mandatory_items and item not in optional_items:
                    ignored_items.append(item)
            if validation_errors:
                raise Exception(f"ERROR: Schema validation for FILE_INFO failed "
                                f"due to the following reasons: {validation_errors} ")
            if ignored_items:
                logging.error(f"Note: The following options are being ignored "
                             f"{ignored_items} for {endpoint_type} file info.")

        logging.info(f"SCHEMA VALIDATION: Done validating file_info section of "
                     f"{endpoint_type_}.")

    @staticmethod
    def validate_credentials(endpoint_json_):
        """
        Validates the optional "credentials" section of schema source/sink data.
        :param endpoint_json_: Data sink/source JSON record.
        :return: None.
        """

        mandatory_items = CredentialsMandatory.__members__
        optional_items = CredentialsOptional.__members__
        credentials_data = endpoint_json_["credentials"]
        ignored_items = []
        validation_errors = []

        for item in mandatory_items:
            if item not in credentials_data:
                validation_errors.append(
                    f"{item} is missing and is mandatory.")
        for item in optional_items:
            if item not in credentials_data:
                credentials_data[item] = None
        for item in credentials_data:
            if item not in mandatory_items and item not in optional_items:
                ignored_items.append(item)
        if validation_errors:
            raise Exception(
                f"ERROR: Schema validation for FILE_INFO failed "
                f"due to the following reasons: {validation_errors} ")
        if ignored_items:
            logging.error(f"Note: The following options are being ignored "
                          f"{ignored_items} for credentials.")

        logging.info(f"SCHEMA VALIDATION: Done validating credentials.")

    @staticmethod
    def get_field_names_for_file(schema_data_json_):
        """
        Parses the schema to find the field names that would likely appear in
        a file being processed with the schema. This is useful for when files
        do not have column headers, and a list needs to be generated to pass
        to other operations.
        :param schema_data_json_: Schema data in JSON format.
        :return: List of field names as strings.
        """

        fields = []
        for field in schema_data_json_["fields"]:
            # Remove added column fields as these would not be in native file
            if not field["type"].get("add_column"):
                fields.append(field.get('name'))
        return fields

    @staticmethod
    def generate_file_output_args(schema_, endpoint_schema_):
        """
        Generates a dictionary of arguments for file operations.
        :param schema_: JSON of data schema.
        :param endpoint_schema_: JSON of endpoint schema.
        :return: Dicitonary of arguments.
        """
        file_type = endpoint_schema_["file_info"]["type"]

        if file_type == "csv":
            args = SDCDataSchema.generate_csv_args(schema_, endpoint_schema_)
            return args
        else:
            None

    @staticmethod
    def generate_csv_args(schema_, endpoint_schema_):
        """
        Private function. Generates a dictionary of arguments at self.args from
        info contained in data schema and other functions, depending on type of
        endpoint being used.
        :param schema_: JSON of data schema.
        :param endpoint_schema_: JSON of endpoint schema.
        :return: Dicitonary of arguments.
        """
        args = {}

        if endpoint_schema_["endpoint_type"] == "sink":
            for k, v in endpoint_schema_["file_info"].items():
                if k in SDCDFCSVMapper.__members__:
                    args[SDCDFCSVMapper[k].value] = endpoint_schema_["file_info"][k]
            args["fieldnames_"] = \
                SDCDataSchema.get_field_names_for_file(schema_)

        elif endpoint_schema_["endpoint_type"] == "source":
            for k, v in endpoint_schema_["file_info"].items():
                if k in PandasCSVMapper.__members__:
                    args[PandasCSVMapper[k].value] = endpoint_schema_["file_info"][k]
            if not endpoint_schema_["file_info"]["headers"]:
                args["names"] = \
                    SDCDataSchema.get_field_names_for_file(schema_)

        return args

    @staticmethod
    def generate_create_table_from_schema(schema_name_, sink_endpoint_tag_):
        """
        Generates Snowflake CREATE TABLE SQL code based on a given schema /
        endpoint.
        :param schema_name_: Name of schema (path + name of file without
        extension).
        :param sink_endpoint_tag_: Tag of sink endpoint.
        :return: String of SQL code.
        """

        file_name = SDCFileHelpers.get_file_path("schema",
                                                 f"{schema_name_}.json")
        schema_data = json.loads(open(file_name).read())
        for sink in schema_data["data_sink"]:
            if sink["tag"] == sink_endpoint_tag_:
                endpoint_data = sink

        sql_code = f'CREATE TABLE "{endpoint_data["database"]}"' \
            f'."{endpoint_data["schema"]}"' \
            f'."{endpoint_data["table_name"]}" \n(\n'
        for field in schema_data["fields"]:
            if "drop_column" in field and field["drop_column"]:
                continue
            if "rename" in field:

                col_name = SnowflakeDatabase.clean_column_name(field["rename"])
            else:
                col_name = SnowflakeDatabase.clean_column_name(field["name"])

            if "logical_type" in field["type"]:
                typing = SchemaLogicalTypeToDatabaseMapper[
                    field["type"]["logical_type"]].value
            else:
                typing = SchemaTypeToDatabaseMapper[field["type"]["type"]].value
            line = f'\t"{col_name}" {typing},\n'
            sql_code += line
        sql_code = sql_code.rstrip(',\n')
        sql_code += "\n);"

        return sql_code

    @staticmethod
    def generate_create_masked_view_from_schema(schema_name_, sink_endpoint_tag_):
        """
        Generates Snowflake CREATE VIEW SQL code based on a given schema /
        endpoint and masks pii data.
        :param schema_name_: Name of schema (path + name of file without
        extension).
        :param sink_endpoint_tag_: Tag of sink endpoint.
        :return: String of SQL code.
        """

        file_name = SDCFileHelpers.get_file_path("schema",
                                                 f"{schema_name_}.json")
        schema_data = json.loads(open(file_name).read())
        for sink in schema_data["data_sink"]:
            if sink["tag"] == sink_endpoint_tag_:
                endpoint_data = sink

        table_name = f'"{endpoint_data["database"]}"' \
            f'."{endpoint_data["schema"]}"."{endpoint_data["table_name"]}"'
        view_name = f'"{endpoint_data["database"]}"' \
            f'."{endpoint_data["schema"]}"."VW_{endpoint_data["table_name"]}"'
        mapping_name = f'"DATAENG_UTILS"."MAPPINGS"."PII_MAPPINGS"'

        sql_code = f'CREATE OR REPLACE VIEW {view_name} COPY GRANTS AS \n(\nSELECT\n'
        for field in schema_data["fields"]:
            if "drop_column" in field and field["drop_column"]:
                continue
            if "rename" in field:
                col_name = SnowflakeDatabase.clean_column_name(field["rename"])
            else:
                col_name = SnowflakeDatabase.clean_column_name(field["name"])

            if "is_pii" in field and field["is_pii"]:
                if "logical_type" in field["type"]:
                    value = SchemaTypeToDatabaseMaskingMapper[field["type"]["logical_type"]].value.format(col_name)
                else:
                    value = SchemaTypeToDatabaseMaskingMapper[field["type"]["type"]].value.format(col_name)

                line = f'CASE WHEN PII_ACCESS = TRUE ' \
                    f'THEN "{col_name}" ELSE {value} ' \
                    f'END AS "{col_name}"'
            else:
                line = f'"{col_name}"'

            sql_code += f'\t{line},\n'

        sql_code = sql_code.rstrip(',\n')
        sql_code += f'\n FROM {table_name}'
        sql_code += f'\n LEFT JOIN {mapping_name} PII ON PII.ROLE = CURRENT_ROLE()\n);'

        return sql_code

