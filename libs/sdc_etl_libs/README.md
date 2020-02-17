## SDCDataframe Schema Design

* [Getting Started](#getting-started)
* [Schema File Location](#file-location)
* [Schema Elements](#schema-elements)
    * [Namespace](#namespace)
    * [Type](#type)
    * [Name](#name)
    * [Country Code](#country-code)
    * [Logging Info](#logging-info)
    * [Transformations](#transformations)
    * [Estimated Row Count](#estimated-row-count)
    * [Estimated Row Size](#estimated-row-size)
    * [Data Source/Sink](#data-sink--data-source)
        * [Type](#endpoint-type)
        * [Tag](#tag)
        * [Endpoint Type](#endpoint-type)
        * [Endpoint-specific Parameters](#endpoint-specific-parameters)
            * [SFTP](#parameters-for-sftp)
            * [S3](#parameters-for-s3)
            * [Snowflake](#parameters-for-snowflake)
            * [API](#parameters-for-api)
        * [Credentials](#credentials)
            * [AWS Secrets](#aws-secrets)
                * [Parameters for AWS Secrets](#parameters-for-awssecret)
                * [How to Store Secret Values](#how-to-store-secret-values)
        * [File Info](#file-info)
            * [File Type-specific Parameters](#file-type-specific-parameters)
                * [File](#parameters-for-file)
                * [CSV](#parameters-for-csv)
                * [Parquet](#parameters-for-parquet)
    * [Fields](#fields)
        * [Types](#types--logical-types)
        * [Logical Types](#types--logical-types)
        * [Optional Parameters](#optional-parameters)
            * [Add Column](#add-column)
            * [Drop Column](#drop-column)
            * [Is Nullable](#is-nullable)
            * [SF Merge Key](#sf-merge-key)
            * [Is PII](#is-pii)
            * [Default Value](#default-value)
* [Complete Schema Example](#complete-schema-example)


## Getting Started
Data schemas are created for use with the SDCDataframe and SDCDataExchange classes. Schemas are JSON-based files that represent the following
types of information about a data set:
* Field names (columns) and their types
* Where the data is located (source) and where we want it to go (sink)
* How to access the data - the location and type of credentials
* What types of transformations we want to apply to a data set
* If the data set is file-based, information related to the files (type, setup, etc.)

The SDCDataframe class uses the schema to generate the dataframe fields and then load the data. If the data does not conform to the schema, an exception will be raised.

The SDCDataExchange class uses the schema to determine how to access the data (data source), where to put it (data sink) and how to move it.

## File Location
Schemas are created and stored as .json files in the [schemas](https://github.com/CamelotVG/data-engineering/tree/master/libs/sdc_etl_libs/schemas) directory,
categorized by the vendor/data source. 

## Schema Elements

---

## Namespace
STRING. Naming used to group a collection of schemas that are related to a data source.

Example use:
```json
"namespace": "Fantasia Data"
```

---

## Type
**CURRENTLY NOT ACTIVE/SUPPORTED**

Proposed use: STRING.

Example use:
```json
"type": "object"
```

---

## Name
STRING. Name of specific schema. Consists of lowercase characters and hyphenated between words. Names must be unique across a [Namespace](#namespace).

Example use:
```json
"name": "event-tracking-data"
```

---

## Country Code
**CURRENTLY NOT ACTIVE/SUPPORTED**

Proposed use: STRING. When a country code is supplied, code will know where data should be stored / transferred to so as to be GDPR, etc. compliant.

Example use:
```json
"country_code": "USA"
```

---

## Logging Info
**CURRENTLY NOT ACTIVE/SUPPORTED**

Proposed use: STRING. Denotes what type of logging method to use when logs are generated with schema use.

Example use:
```json
"logging_info": "snowflake"
```

---

## Transformations
**CURRENTLY NOT ACTIVE/SUPPORTED**

Proposed use: LIST. Denotes transformations to be performed at the the dataframe level. Examples:
* Rename columns to remove special characters not supported by a database sink.
* Encrypt PII data (mask, etc.).

Example use:
```json
"transformations": [{"name": "encrypt_pii", "args": {"encryption_type": "md5"}}]
```

---

## Estimated Row Size
**CURRENTLY NOT ACTIVE/SUPPORTED**

Proposed use: STRING. Estimated size of data (in bytes) that would be loaded to dataframe. This, used in conjunction with [Estimated Row Count](#estimated-row-count),
would automatically trigger what type of dataframe to use (Pandas, dask, Spark, etc.)

Example use:
```json
"estimated_row_size": "300b"
```

---

## Estimated Row Count
**CURRENTLY NOT ACTIVE/SUPPORTED**

Proposed use: INT. Estimated number of rows that would be loaded to dataframe. This, used in conjunction with [Estimated Row Size](#estimated-row-size),
would automatically trigger what type of dataframe to use (Pandas, dask, Spark, etc.)

Example use:
```json
"estimated_row_count": 1000000
```

---

## Data Sink / Data Source
LIST. Required. A list of data sink or source objects that each describe a specific data sink or source.

Example use of data source:
```json
"data_source": [
      {
        "type": "sftp",
        "tag": "alternate_source_0",
        "endpoint_type": "source",
        "host": "ftp.site.com",
        "port": 22,
        "path": "/data/path/",
        "credentials": {
          "type": "awssecrets",
          "name": "vendor/sftp"
        },
        "file_info": {
          "type": "csv",
          "delimiter": "|",
          "headers": false,
          "compression_type": "gz",
          "file_regex": ".*ECS"
        }
      }
```

Example use of data sink:
```json
"data_sink": [
      {
        "type": "s3",
        "tag": "SDC_sink_0",
        "endpoint_type": "sink",
        "bucket": "sdc-bucket-name",
        "prefix": "bucket-prefix/",
        "region": "us-east-2",
        "file_info": {
          "type": "csv",
          "delimiter": ",",
          "file_regex": ".*csv",
          "headers": false
        }
      }
```

#### Type (Exchange type)
STRING. The type of endpoint that this sink/source represents.

Example use:
```json
"type": "sftp"
```

Currently supported types:

Type|Description|
----|------------|
api| API
sftp| SFTP site
s3| S3 bucket
snowflake| Snowflake database

#### Tag
STRING. Human-readable tag of an endpoint. Must be unique across all data sources and sinks in a schema. The SDCDataExchange uses these tags to grab the correct source and sink information.

While the tag can be named anything, the following convention is in place:

Tag|Endpoint Type|Description
---|-------------|-----------
main_source|source|Main source of data. Only 1 allowed.
local_backup_source|source|Backup of source data. Only 1 allowed.
alternate_source_N|source|Additional sources of data, where N is an INT between 0-100.
customerName_sink_N|sink|Data sink, where customerName is SDC or vendor name, and N is an INT between 0-100.

Example use for source:
```json
"tag": "main_source"
```

Example use for sink:
```json
"tag": "FedEx_sink_0"
```

#### Endpoint Type
STRING. Classifies a data location as either a source or sink. The SDCDataExchange uses this to determine how to handle a given endpoint. 

Example use:
```json
"endpoint_type": "source"
```

#### Endpoint-specific Parameters
VARIOUS. Depending on the [Type](#type) and [Endpoint Type](#endpoint-type), additional information will be needed for the schema to be properly validated. 
Mandatory and optional parameters for each type/endpoint type combination can be found in the [SDCDataSchemaEnums](https://github.com/CamelotVG/data-engineering/blob/master/libs/sdc_etl_libs/sdc_data_schema/SDCDataSchemaEnums.py) class.

##### Parameters for SFTP:

Key|Data Type|Source Requirement|Sink Requirement|Description|Notes
---|---------|------------------|----------------|-----------|-----
host|str|Mandatory|Mandatory|Host name of SFTP site|
port|int|Mandatory|Mandatory|Port number of SFTP site|
path|str|Mandatory|Mandatory|Path to directory containing files needed|

##### Parameters for S3:

Key|Data Type|Source Requirement|Sink Requirement|Description|Notes
---|---------|------------------|----------------|-----------|-----
bucket|str|Mandatory|Mandatory|S3 bucket name|
region|str|Mandatory|Mandatory|AWS region|
prefix|str|Optional|Optional|S3 bucket prefix (i.e., path to directory containing files needed|

##### Parameters for Snowflake:

Key|Data Type|Source Requirement|Sink Requirement|Description|Notes
---|---------|------------------|----------------|-----------|-----
database|str|Mandatory|Mandatory|Name of database|
table_name|str|Mandatory|Mandatory|Name of table in database|
schema|str|Mandatory|Mandatory|Name of schema in database|
upsert|boolean|N/A|Optional|If true, data will be upserted/merged into sink table. If false, or parameter is absent, data will be inserted|
dedupe|boolean|N/A|Optional|If true, data from a merge will be deduped prior to being merged into the permanent table|Deduping is done by running "select distinct * from ..."
bookmark_filenames|boolean|N/A|Optional|If true, the ETL_FILES_LOADED table will be used in the data exchange for determining what files to exchange and record those that are loaded||
write_filename_to_db|boolean|N/A|Optional|If true, and source is file-based, then the filename will be written to the "_ETL_FILENAME" column of the Snowflake table (Column must be included in Snowflake table schema as type VARCHAR and defined in SDCDataframe schema fields section as type string with "add_column": true)||
sql_file_path|str|Mandatory|N/A|Path (from the "sql" folder in sdc_etl_libs") and file name of SQL file that will be used to pull the data that will be moved to the sink||


##### Parameters for API:

Key|Data Type|Source Requirement|Sink Requirement|Description|Notes
---|---------|------------------|----------------|-----------|-----
base_url|str|Mandatory|N/A|Base URL to make API calls to|


Example use for SFTP endpoint:
```json
"host": "ftp.site.com",
"port": 22,
"path": "/data/path/"
```

Example use for S3 endpoint:
```json
"bucket": "sdc-bucket-name",
"prefix": "bucket-prefix/",
"region": "us-east-2"
```

#### Credentials
DICT. Optional method for retrieving credentials for a data sink/source.

Parameters: 

Key|Data Type|Source Requirement|Sink Requirement|Description|Notes
---|---------|------------------|----------------|-----------|-----
type|str|Varies based on type/endpoint type|Varies based on type/endpoint type|Type of credential store to use|
name|str|Varies based on type/endpoint type|Varies based on type/endpoint type|Name of credential store|

Currently supported types:

Type|Description|Notes
----|-----------|-----
awssecrets| Will retrieve secret values from AWS for the secret name given.| The IAM user/role accessing the secret must have the proper access.

##### AWS Secrets

##### Parameters for "awssecrets"

Key|Data Type|Source Requirement|Sink Requirement|Description|Notes
---|---------|------------------|----------------|-----------|-----
name|str|Varies based on type/endpoint type|Varies based on type/endpoint type|Name of credential store|

##### How to Store Secret Values

When using awssecrets as a credentials type, the secret itself must have specific key names for values for
the SDCDataExchange to work. Below are the following naming conventions:

SFTP Secret Keys:

Key|Description|Requirement|
---|-----------|-----------|
username|SFTP username|Mandatory
password|SFTP password|Optional (Needed if no rsa_key)
rsa_key|SFTP password|Optional (Needed if no password)
host|SFTP hostname|Optional
port|SFTP port number|Optional

S3 Secret Keys:

Key|Description|Requirement|
---|-----------|-----------|
access_key|S3 access key|Mandatory
secret_key|S3 secret key|Mandatory

Snowflake Secret Keys:

Key|Description|Requirement|
---|-----------|-----------|
username|Snowflake username|Mandatory
password|Snowflake password|Mandatory
account|Snowflake account|Mandatory
warehouse|Snowflake warehouse (for compute)|Mandatory
role|Snowflake role (for access)|Mandatory


Example use:
```json
"credentials": {
  "type": "awssecrets",
  "name": "vendor/sftp"
  }
```


#### File Info
DICT. Required if endpoint type is file-based (e.g. SFTP, S3). Gives information about file typing, structure and which files to look for so that SDCDataExchange can properly process.
Mandatory and optional parameters are defined in the [SDCDataSchemaEnums](https://github.com/CamelotVG/data-engineering/blob/master/libs/sdc_etl_libs/sdc_data_schema/SDCDataSchemaEnums.py) class.

Parameters:

Key|Data Type|Source Requirement|Sink Requirement|Description|Notes
---|---------|------------------|----------------|-----------|-----
type|str|Mandatory|Mandatory|Type of file (e.g. csv)|

Currently supported types:

Type|Description|Notes|
----|-----------|-----|
file|Any file - move without schema validation||
csv| CSV files||
parquet| Parquet files||


#### File Type-specific Parameters
VARIOUS. Depending on the [exchange type](#type-exchange-type), [endpoint tyoe](#endpoint-type) and file type, additional information may be needed for the schema to be properly validated. 
Mandatory and optional parameters for each combination can be found in the [SDCDataSchemaEnums](https://github.com/CamelotVG/data-engineering/blob/master/libs/sdc_etl_libs/sdc_data_schema/SDCDataSchemaEnums.py) class.

##### Parameters for "file": 

Key|Data Type|Source Requirement|Sink Requirement|Description|Notes
---|---------|------------------|----------------|-----------|-----
file_regex|str|Mandatory|Mandatory|Regex that describes the file names SDCDataExchange should pull|


##### Parameters for "csv": 

Key|Data Type|Source Requirement|Sink Requirement|Description|Notes
---|---------|------------------|----------------|-----------|-----
file_regex|str|Mandatory|Mandatory|Regex that describes the file names SDCDataExchange should pull|
delimiter|str|Mandatory|Mandatory|Delimiter of file (e.g. ',')|
headers|bool|Mandatory|Mandatory|If source: if file has headers. If sink: if headers should be written to sink file|
decode|str|Optional|Optional|Character decoding to use for a file|
compression_type|str|Optional|N/A|The compression type of the file| Available Values:  ‘gzip’, ‘bz2’, ‘zip’, ‘xz’
file_name|str|N/A|Optional|File name to write data to when source is a database and sink is file-based|

##### Parameters for "parquet": 

Key|Data Type|Source Requirement|Sink Requirement|Description|Notes
---|---------|------------------|----------------|-----------|-----
file_regex|str|Mandatory|Mandatory|Regex that describes the file names SDCDataExchange should pull|


Example use:
```json
"file_info": {
  "type": "csv",
  "delimiter": "|",
  "headers": false,
  "file_regex": ".*txt"
  }
```

---

## Fields

LIST. Listing of fields that describe the data, including:
* Fields present within the data source. Examples:
    * Fieldnames in a CSV file.
    * JSON keys from an API response.
* Fields that will be added to the dataframe (using the [Add Column](#add-column) parameter). Example uses:
    * Adding timestamp of when data was inserted into database.
    * Adding name of file data came from.

Parameters:

Key|Data Type|Description|Notes
---|---------|-----------|-----
name|str|Name of the field|
type|dict|Data type for the field (type/logical type|
transformations|list|Transformations to apply to the column in the dataframe|NOT YET ACTIVE/SUPPORTED


Example use:
```json
"fields": [
      {"name": "FOO", "type": {"type": "string"}, "transformations": ["removeWhiteSpace"]},
      {"name": "FIZZ_DATE", "type": {"type": "string", "logical_type": "datetime"}},
      {"name": "BAR", "type": {"type": "int"}},
      {"name": "BAZ", "type": {"type": "boolean"}},
      {"name": "BAR_FIZZ", "type": {"type": "float"}}
      ]
```

#### Types / Logical Types
STRING. The typing of the field and how the data will be loaded into a dataframe. When given a supported logical type, additonal events are triggered to transform the data into the desired type.

Currently supported values:

Type|Logical Type|Result Data Type
----|------------|-----------
string| | Numpy Object
string| datetime| Numpty Datetime64
string| json| Numpy Object 
int| | Pandas Int32
float| | Numpy Float32
double| | Numpy Float64
long| | Pandas Int64
boolean| | Numpy Bool

#### Optional Field Parameters

##### Add Column
BOOLEAN. When used - and value is true - the column will be added to the dataframe and a default type will be set (depending on the field type specified). Data can then
be assigned/loaded to the column through a separate process.

Example use:
```json
{"name":"_ETL_FILENAME","type": {"type":"string"}, "add_column": true}
```

##### Drop Column
BOOLEAN. When used - and value is true - column will be automatically dropped from dataframe after data is loaded. Useful when needing to prevent
certain sensitive data points from being moved to a data sink.

Example use:
```json
{"name":"COLUMN20","type": {"type":"string"}, "drop_column": true}
```

##### Is Nullable
BOOLEAN. When used - and value is true - the field is allowed to be missing from the source data being loaded into the dataframe. Useful when:
* A value/key is not always present when making an API call, but we want it captured when present.
* A data schema has evolved over time, and fields have been dropped or added.

Example use:
```json
{"name":"COLUMN20","type": {"type":"string"}, "is_nullable": true}
```

##### SF Merge Key
BOOLEAN. When used - and value is true - this field will be used as an upsert key when merging to 
Snowflake. This will only work if used with a sink where "upsert" is present and set to true. 
There can be one or more sf_merge_keys set. If none are set, and upsert is true, an error will be thrown.

```json
{"name":"COLUMN20","type": {"type":"string"}, "sf_merge_key": true},
{"name":"COLUMN21","type": {"type":"string"}, "sf_merge_key": true},
{"name":"COLUMN22","type": {"type":"string"}}
```


##### Is PII
BOOLEAN. When used - and value is true - the following events will occur for the field:
- When generating a masked view via the SDCDataSchema class, the field's value will be masked with sha256() if the current role being used 
does not have sufficient privilege to access the data.

Example use:
```json
{"name":"COLUMN20","type": {"type":"string"}, "is_pii": true}
```


##### Default Value
VARIOUS. Sets the value of this column to the default value given if the value is missing in the SDCDataframe after 
loading and processing (i.e. is None or np.NaN)

Example use:
```json
{"name":"QUEUE_TIME","type": {"type":"string"}, "default_value": "00:00:00"}
```


##### Rename
STRING. Renames a column in the SDCDataframe from the raw data name brought in to the name provided here. 
Useful when a dataset has ambiguous column names.

Example use:
```json
{"name":"COL123", "rename": "Customer Zip", "type": {"type":"string"}}
```




---


## Complete Schema Example

```json
  {
    "namespace": "Name of data source",
    "type": "object",
    "name": "Name of data set",
    "country_code": "USA",
    "logging_info": "snowflake",
    "transformations": [{"name":"encrypt_pii", "args": {"encryption_type": "md5"}}],
    "estimated_row_size": "300b",
    "estimated_row_count": 1000000,
    "data_sink": [
      {
        "type": "s3",
        "tag": "SDC_sink_0",
        "endpoint_type": "sink",
        "bucket": "sdc-bucket-name",
        "prefix": "bucket-prefix/",
        "region": "us-east-2",
        "file_info": {
          "type": "csv",
          "delimiter": ",",
          "file_regex": ".*csv",
          "headers": false,
          "no_schema_validation": true
        }
      },
      {
        "type": "sftp",
        "tag": "VendorName_sink_0",
        "endpoint_type": "sink",
        "host": "0.0.0.0",
        "port": 22,
        "path": "/data/path/",
        "credentials": {
          "type": "awssecrets",
          "name": "vendor/sftp"
        },
        "file_info": {
          "type": "csv",
          "delimiter": ",",
          "file_regex": ".*csv",
          "headers": false
        }
      },
      {
        "type": "snowflake",
        "tag": "SDC_sink_1",
        "endpoint_type": "sink",
        "database": "ANALYTICS",
        "table_name": "EVENTS",
        "schema": "TNT",
        "write_filename_to_db": true,
        "upsert": false,
        "credentials": {
          "type": "awssecrets",
          "name": "snowflake/service_account/airflow"
        }
      },
    ],
    "data_source": [
       {
        "type": "api",
        "tag": "main_source",
        "endpoint_type": "source",
        "url": "foo.com"
      },
      {
        "type": "sftp",
        "tag": "alternate_source_0",
        "endpoint_type": "source",
        "host": "ftp.site.com",
        "port": 22,
        "path": "/data/path/",
        "credentials": {
          "type": "awssecrets",
          "name": "vendor/sftp"
        },
        "file_info": {
          "type": "csv",
          "delimiter": "|",
          "headers": false,
          "file_regex": ".*ECS"
        }
      },
      {
        "type": "s3",
        "tag": "alternate_source_1",
        "endpoint_type": "source",
        "bucket": "sdc-bucket-name",
        "prefix": "bucket-prefix/",
        "region": "us-east-2",
        "file_info": {
          "type": "csv",
          "delimiter": ",",
          "headers": false,
          "file_regex": ".*csv"
        }
      }
    ],
    "fields": [
      {"name": "FOO", "type": {"type": "string"}, "transformations": ["removeWhiteSpace"]},
      {"name": "FIZZ_DATE", "type": {"type": "string", "logical_type": "datetime"}, "transformations": [{"name": "to_timezone", "args": {"from_": "pst", "to_": "utc"}}]},
      {"name": "BAR", "type": {"type": "int"}},
      {"name": "BAZ", "type": {"type": "boolean"}},
      {"name": "BAR_FIZZ", "type": {"type": "float"}},
      {"name": "BAZ_FOO", "type": {"type":"string", "logical_type": "email"}, "transformations": ["scrub_pii"], "is_pii": true},
      {"name": "FOO_BIZZ", "type": {"type":"double"}},
      {"name": "BAR_BAZ", "type": {"type":"long"}},
      {"name": "BAZ_FOO", "type": {"type":"string", "logical_type": "phone_number"}, "transformations": ["scrub_pii"], "is_pii": true},
      {"name": "BAZ_FIZZ", "type": {"type":"string", "logical_type": "address"}, "is_pii": true, "drop_column": true},
      {"name": "_SF_INSERTEDDATETIME","type": {"type":"string", "logical_type": "datetime"}, "add_column": true}
    ]
  }
```





