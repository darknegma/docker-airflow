## SDCDataExchange

* [Getting Started](#getting-started)
* [Setup Example](#setup-example)
* [Determining What Data to Exchange](#determining-what-data-to-exchange)
* [Setting up Source/Sinks for Development](#setting-up-sourcesinks-for-development-environments)
* [Logging Results](#logging-results)
* [What To Do: Common Scenarios](#what-to-do-common-scenarios)
    * [Source Files Do Not Have Headers](#source-files-do-not-have-headers)
    * [Need to Write to Multiple Sinks](#need-to-write-to-multiple-sinks)
    * [Need to Rename Headers](#need-to-rename-headers)
* [Airflow](#airflow)
    * [Airflow DAG Example](#airflow-dag-example)
    * [Airflow E-mail Example](#airflow-dag-example)

 
## Getting Started

The SDCDataExchange class is used to exchange data between a source and one (or more) sinks. It relies heavily on the following classes:
* [SDCDataSchema](https://github.com/CamelotVG/data-engineering/blob/master/libs/sdc_etl_libs/sdc_data_schema/SDCDataSchema.py): 
To validate that the data schema meets all the parameter requirements for the exchange type, endpoint type and (if applicable) file type.
* [SDCDataframe](https://github.com/CamelotVG/data-engineering/blob/master/libs/sdc_etl_libs/sdc_dataframe/Dataframe.py): 
To validate that the data recevied conforms to a data schema and to run any necessary processing / transformations on the data.


## Setup Example

With a properly setup schema, exchanging data between two verified endpoints takes only a few lines of code:

```python
from sdc_etl_libs.sdc_data_exchange.SDCDataExchange import SDCDataExchange

exchange = SDCDataExchange(
    schema_name_="HFD/daily-charges", 
    source_endpoint_tag_="main_source", 
    sink_endpoint_tag_="SDC_sink_0")

result = exchange.exchange_data()
```

Log results:
```text
In[7]: print(result)
Out[8]: 
{'SDC_sink_0': [
  'SUCCESS: Loaded SMILE_DIRECT_AU_2019-12-03-040001.txt to snowflake. 36 row(s) inserted. 1 row(s) upserted.',
  'SUCCESS: Loaded SMILE_DIRECT_AU_2019-12-04-040002.txt to snowflake. 30 row(s) inserted. 0 row(s) upserted.',
  'SUCCESS: Loaded SMILE_DIRECT_AU_2019-12-05-040003.txt to snowflake. 10 row(s) inserted. 5 row(s) upserted.',
  'EMPTY: 2019-07-28-210001.TXT contains no data and was skipped.',
  'ERROR: Syncing Processed_barrett_SDC_Outbound_Events_2018-12-05-083600.txt to snowflake failed.'
   ]
}
```

Initializing an SDCDataExchange object requires the following arguments:
* schema_name_: The directory path and file name (without extension) of the data's schema.
* source_endpoint_tag_: Tag value of the data source (From "data_source" section of schema)
* sink_endpoint_tag_: Tag value of the data source (From "data_sink" section of schema). 
This can also be a list of multiple tag names for the data source to transfer data to.

## Determining What Data to Exchange

The SDCDataExchange automatically determines what data to exchange between the sink and source based on the information
provided in the schema. 

When both the source and sink are file-based systems (e.g. S3, SFTP), the data exchange will generate a list of file names from both 
endpoints and compare the results. Data will be moved if the file exists in the source but **not** in the sink. The "file_regex" 
parameters in the data schema control what files the data exchange will look for. It is important to note that 
**a file will not be re-processed if it already exists in the sink**. If a file disappears/is deleted from the sink and still exists in the 
source, it will be re-processed.

When Snowflake is the sink (and the source is file-based), an ETL_FILES_LOADED table needs to be setup in the database schema that 
the sink tables are located in.
This will act as the "list of files" that the data exchange will use to compare against files in the source. The code to setup
that table is below:

```sql
create table "ETL_FILES_LOADED" (

    "TABLE_NAME" varchar,
    "FILE_NAME" varchar,
    "DATE_LOADED" datetime

);
```

If a file that was successfully processed needs to be re-processed in Snowflake, the file name will need to be deleted from the
ETL_FILES_LOADED table first.

```sql
delete from  "ETL_FILES_LOADED" where "FILE_NAME" = "FileNameABC.csv";
```


## Setting up Source/Sinks for Development Environments

To create development endpoints for testing purposes, 
include an additional entry for a source or sink with a "_dev"-suffixed tag name and the appropriate value changes. These tags
can then be passed to SDCDataExchange() to source/sink to/from the development environment.

```json

"data_sink": [
      {
        "type": "snowflake",
        "tag": "SDC_sink_0",
        "endpoint_type": "sink",
        "database": "LOGISTICS",
        "table_name": "INBOUND_EVENTS",
        "schema": "BARRETT",
        "write_filename_to_db": true,
        "upsert": false,
        "credentials": {
          "type": "awssecrets",
          "name": "snowflake/service_account/airflow"
        }
      },
      {
        "type": "snowflake",
        "tag": "SDC_sink_0_dev",
        "endpoint_type": "sink",
        "database": "DATAENGINEERING",
        "table_name": "INBOUND_EVENTS",
        "schema": "BARRETT",
        "write_filename_to_db": true,
        "upsert": false,
        "credentials": {
          "type": "awssecrets",
          "name": "snowflake/service_account/airflow"
        }
      }
    ]
    
```

---
## Logging Results
For each source data (file or database source) exchanged between a sink, a log message will be generated. 
There are three possible categories of messages:

SUCCESS message:
The data was exchanged successfully between the source and sink. 
When the source is file-based, and Snowflake is the sink, the ETL_FILES_LOADED table in the associated database schema
will be updated with the file name so that the file is not re-processed again.
```text
'SUCCESS: Loaded SMILE_DIRECT_AU_2019-12-03-040001.txt to snowflake. 36 row(s) inserted. 1 row(s) upserted.'
```

ERROR message:
There was an error exchanging the data between the source and sink. Check the Python generated logs for the cause.
When the source is file-based, and Snowflake is the sink, the ETL_FILES_LOADED table will not be updated with the file name. 
SDCDataExchange will attempt to re-process the file then next time it runs.
```text
'ERROR: Syncing Processed_barrett_SDC_Outbound_Events_2018-12-05-083600.txt to snowflake failed.'
```

EMPTY message:
When the source is file-based, and the file is empty (Either there is no data or just ar row with headers) an EMPTY 
message will be generated. When the source is file-based, and Snowflake is the sink, the ETL_FILES_LOADED table will not be updated with the file name. 
SDCDataExchange will attempt to re-process the file then next time it runs.
```text
'EMPTY: 2019-07-28-210001.TXT contains no data and was skipped.'
```

---

## What To Do: Common Scenarios

### Source Files Do Not Have Headers

When a file-based source contains files that do not have headers in them, set the "headers" parameter in the data scheam
to false (for the source). The SDCDataframe class will assume the order of the columns in the file match the names/order 
of the fields in the data schema. To write these headers out to the sink, set the "headers" parameter to "true".

Data Source (When headers are missing):
```json
"file_info": {
  "type": "csv",
  "delimiter": ",",
  "file_regex": ".*",
  "headers": false
}
```

Data Sink (To write headers out):
```json
"file_info": {
  "type": "csv",
  "delimiter": ",",
  "file_regex": ".*",
  "headers": true
}
```

### Need to Write to Multiple Sinks

It is possible to write to multiple sinks from the same source in one run of exchanging data. To do this,
pass a list of data sink tags to the source_endpoint_tag_ argument:

```python
exchange = SDCDataExchange(
        "TNT/tracking-events", 
        source_endpoint_tag_="main_source", 
        sink_endpoint_tag_=["SDC_sink_0", "Vendor_sink_0"])
```

When the exchange is triggered, the data exchange will produce a collective list of all files 
missing from one or more of the sinks. Each file will then be iterated over with each sink
checked to see if the file is missing there.


### Need to Rename Headers

Currently, there is no way to change field names via the data schema. It is proposed to add a new parameter, such as "rename_to"
that can be used to do this. 

When the sink is Snowfalke, the columns are automatically renamed from the source names to be more database-friendly version.
This is done through the cleanse_column_names function in SDCDataframe. For example:


"Default #: Addresses : Country (Full Name)" 

will be converted to

"DEFAULT_NUM_ADDRESSES__COUNTRY_FULL_NAME" 

before being inserted into Snowflake. The tables need to be setup accordingly.

---
## Airflow

### Airflow DAG Example

```python

"""
STEP 1: Import all the necessary modules for Airflow / SDCETLLibs
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from sdc_etl_libs.airflow_helpers.AirflowHelpers import AirflowHelpers
from sdc_etl_libs.sdc_data_exchange.SDCDataExchange import SDCDataExchange


"""
STEP 2: Create the dictionaries that will contain each exchange functions source and sinks 
tags for production and development environments.

The correct naming convetion is to name the dictionaries as the function/task names they 
will be used for, with "_args" suffixed on.
"""
if Variable.get("environment") == "development":
    daily_charges_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    invalid_card_accounts_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0_dev"}
    
elif Variable.get("environment") == "production":
    daily_charges_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}
    invalid_card_accounts_to_db_args = {"source_": "main_source", "sink_": "SDC_sink_0"}


"""
STEP 3: Create the functions that will be passed to the Airflow tasks later on. The 
sink/source kwargs would be left as is below. The values will be passed in from the 
above dictionaries when the tasks run.
"""
def daily_charges_to_db(**kwargs):

    exchange = SDCDataExchange("HFD/daily-charges", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    """
    NOTE: This part takes the results log dictionary that the SDCDataExchange class generates,
    puts it into an HTML-friendly format, and sends to Airflow's xcoms for the e-mail task to process.
    The first argument describes what this exchange was, and will be used as a header in the e-mail
    """
    AirflowHelpers.process_etl_results_log(
        "Daily Charges data from S3 to Snowflake", result, **kwargs)

def invalid_card_accounts_to_db(**kwargs):

    exchange = SDCDataExchange(
        "HFD/invalid-card-accounts", kwargs["source_"], kwargs["sink_"])
    result = exchange.exchange_data()

    AirflowHelpers.process_etl_results_log(
        "Invalid Card Accounts data from S3 to Snowflake", result, **kwargs)


"""
STEP 4: Create the default args for the Airflow DAG to use and the DAG itself. 
For file-based exchanges, it's recommended to set max_active_runs to 1, so that multiple DAG runs
do not inadvertently try to process the same files the same time.
"""
default_args = {
    'owner': 'trevor.wnuk',
    'start_date': datetime(2019, 10, 15),
    'email': AirflowHelpers.get_dag_emails("data-eng"),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl-hfd',
    default_args=default_args,
    schedule_interval='45 15 * * *',
    dagrun_timeout=timedelta(hours=2),
    max_active_runs=1
)


"""
STEP 5: Create the tasks (one for each of the functions created in STEP 3, using Airflow's 
PythonOperator. For op_kwargs, the dictionary of prod/dev sink and source tags that were 
created in STEP 2 must be passed in.
"""
run_daily_charges_to_db = PythonOperator(
    task_id='run_daily_charges_to_db',
    provide_context=True,
    python_callable=daily_charges_to_db,
    op_kwargs=daily_charges_to_db_args,
    dag=dag)

run_invalid_card_accounts_to_db = PythonOperator(
    task_id='run_invalid_card_accounts_to_db',
    provide_context=True,
    python_callable=invalid_card_accounts_to_db,
    op_kwargs=invalid_card_accounts_to_db_args,
    dag=dag)


"""
STEP 6: Setup the e-mail tasks so that the SDCDataExchange log results are e-mailed out.
The only part that needs to be changed here is the "etl_name_", which will be used in 
the subject line of the e-mail.
"""
generate_email = PythonOperator(
    task_id='generate_email',
    provide_context=True,
    op_kwargs={
            'etl_name_': "HFD",
            'tasks_': [task.task_id for task in dag.tasks],
            'environment_': Variable.get("environment")
        },
    python_callable=AirflowHelpers.generate_data_exchange_email,
    dag=dag)

send_email = EmailOperator(
    task_id='send_email',
    to=AirflowHelpers.get_dag_emails(dag.dag_id),
    retries=0,
    dag=dag,
    subject="{{ task_instance.xcom_pull(task_id s='generate_email', key='email_subject') }}",
    html_content="{{ task_instance.xcom_pull(task_ids='generate_email', key='email_body') }}"
)


"""
STEP 7: Setup the order/dependencies for the tasks. 
generate_email and send_email should look like the below 
and should always run after all other tasks have finished.
"""
(
    run_daily_charges_to_db,
    run_invalid_card_accounts_to_db
) >> generate_email >> send_email

```


### Airflow E-mail Example

![Airflow E-mail Example](/libs/sdc_etl_libs/sdc_data_exchange/sdcdataexchange_email_example.png)






