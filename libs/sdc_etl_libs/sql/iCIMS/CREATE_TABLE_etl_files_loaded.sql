use role "AIRFLOW_SERVICE_ROLE";
use database "HRIS_DATA";
use schema "ICIMS";

create table "ETL_FILES_LOADED"
(

    "TABLE_NAME"  varchar,
    "FILE_NAME"   varchar,
    "DATE_LOADED" datetime

);