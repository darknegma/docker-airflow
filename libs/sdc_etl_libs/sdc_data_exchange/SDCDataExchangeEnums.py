
from enum import Enum


class DataExchangeTypes(Enum):
    sftp = "sftp"
    s3 = "s3"
    db = "db"


class FileExchangeTypes(Enum):
    sftp = "sftp"
    s3 = "s3"


class DatabaseExchangeTypes(Enum):
    snowflake = "snowflake"


class APIExchangeTypes(Enum):
    api = "api"


class FileResultTypes(Enum):
    error = "ERROR"
    success = "SUCCESS"
    empty = "EMPTY"





