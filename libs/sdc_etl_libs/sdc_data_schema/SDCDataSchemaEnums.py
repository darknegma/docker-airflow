
from enum import Enum

#SCHEMA LEVEL Enums #########################
# S3 Source
class S3SourceInfoMandatory(Enum):
    type = "type"
    bucket = "bucket"
    file_info = "file_info"
    tag = "tag"
    endpoint_type = "endpoint_type"
    region = "region"


class S3SourceInfoOptional(Enum):
    prefix = "prefix"
    credentials = "credentials"


# S3 Sink
class S3SinkInfoMandatory(Enum):
    type = "type"
    bucket = "bucket"
    file_info = "file_info"
    tag = "tag"
    endpoint_type = "endpoint_type"
    region = "region"


class S3SinkInfoOptional(Enum):
    prefix = "prefix"
    credentials = "credentials"


# S3 Data Types
class S3InfoDataTypes(Enum):
    pass


# SFTP Source
class SFTPSourceInfoMandatory(Enum):
    type = "type"
    host = "host"
    port = "port"
    path = "path"
    file_info = "file_info"
    tag = "tag"
    endpoint_type = "endpoint_type"
    credentials = "credentials"


class SFTPSourceInfoOptional(Enum):
    pass


# SFTP Sink
class SFTPSinkInfoMandatory(Enum):
    type = "type"
    host = "host"
    port = "port"
    path = "path"
    file_info = "file_info"
    tag = "tag"
    endpoint_type = "endpoint_type"
    credentials = "credentials"


class SFTPSinkInfoOptional(Enum):
    pass


# SFTP Data Types
class SFTPInfoDataTypes(Enum):
    port = int


# Snowflake Source
class SnowflakeSourceInfoMandatory(Enum):
    type = "type"
    database = "database"
    table_name = "table_name"
    schema = "schema"
    tag = "tag"
    endpoint_type = "endpoint_type"
    credentials = "credentials"
    sql_file_name = "sql_file_name"


class SnowflakeSourceInfoOptional(Enum):
    pass


# Snowflake Sink
class SnowflakeSinkInfoMandatory(Enum):
    type = "type"
    database = "database"
    table_name = "table_name"
    schema = "schema"
    tag = "tag"
    endpoint_type = "endpoint_type"
    credentials = "credentials"


class SnowflakeSinkInfoOptional(Enum):
    upsert = "upsert"
    dedupe = "dedupe"
    bookmark_filenames = "bookmark_filenames"
    write_filename_to_db = "write_filename_to_db"


# Snowflake Data Types
class SnowflakeInfoDataTypes(Enum):
    upsert = bool
    dedupe = bool
    bookmark_filenames = bool
    write_filename_to_db = bool


# File Info Source - File
class FileInfoFileSourceMandatory(Enum):
    type = "type"


class FileInfoFileSourceOptional(Enum):
    decode = "decode"


# File Info Sink - File
class FileInfoFileSinkMandatory(Enum):
    type = "type"



class FileInfoFileSinkOptional(Enum):
    decode = "decode"
    file_name = "file_name"

# File Info Source - CSV
class FileInfoCSVSourceMandatory(Enum):
    type = "type"
    file_regex = "file_regex"
    delimiter = "delimiter"
    headers = "headers"


class FileInfoCSVSourceOptional(Enum):
    decode = "decode"
    compression_type = "compression_type"


# File Info Sink - CSV
class FileInfoCSVSinkMandatory(Enum):
    type = "type"
    file_regex = "file_regex"
    delimiter = "delimiter"
    headers = "headers"



class FileInfoCSVSinkOptional(Enum):
    decode = "decode"
    file_name = "file_name"

# File Info Source - Parquet
class FileInfoParquetSourceMandatory(Enum):
    type = "type"


class FileInfoParquetSourceOptional(Enum):
    file_name = "file_name"


# File Info Sink- Parquet
class FileInfoParquetSinkMandatory(Enum):
    type = "type"



class FileInfoParquetSinkOptional(Enum):
    file_name = "file_name"


# File Info Data Types
class FileInfoDataTypes(Enum):
    headers = bool
    decode = str


# Credentials
class CredentialsMandatory(Enum):
    type = "type"


class CredentialsOptional(Enum):
    name = "name"

#Code level args enums##############################
class PandasCSVMapper(Enum):
    """
    Mapping for:
    Data schema file info key -> pd.from_csv function parameter name.
    """
    delimiter = "delimiter"
    headers = "header"
    compression_type = "compression"


class SDCDFCSVMapper(Enum):
    """
    Mapping for:
    Data schema file info key -> SDCDataframe function parameter name.
    """
    delimiter = "delimiter_"
    headers = "headers_"


class SchemaTypeToDatabaseMapper(Enum):
    """
    Mapping for:
    Data schema file field type > Snowflake database data type
    """
    string = "VARCHAR"
    int = "INT"
    float = "FLOAT"
    boolean = "BOOLEAN"
    double = "DOUBLE"


class SchemaLogicalTypeToDatabaseMapper(Enum):
    """
    Mapping for:
    Data schema file field logical type > Snowflake database data type
    """
    json = "VARIANT"
    datetime = "DATETIME"


class SchemaTypeToDatabaseMaskingMapper(Enum):
    """
    Mapping for:
    Data schema file field type > Snowflake CREATE VIEW masked value
    """
    string = 'sha2("{0}", 512)'
    int = '-9'
    float = '-9.99'
    boolean = 'NULL'
    json = 'NULL'
    datetime = '"2099-12-31"'