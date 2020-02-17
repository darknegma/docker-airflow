use role "AIRFLOW_SERVICE_ROLE";
use database "RAW";
use schema "LOGIC_PLUM";

create table "ETL_FILES_LOADED"
(

    "TABLE_NAME"  varchar,
    "FILE_NAME"   varchar,
    "DATE_LOADED" datetime

);