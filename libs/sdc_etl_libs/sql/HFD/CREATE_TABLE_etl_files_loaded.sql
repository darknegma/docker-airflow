use role "AIRFLOW_SERVICE_ROLE";
use database "FINANCE";
use schema "HFD";

create table "ETL_FILES_LOADED" (

    "TABLE_NAME" varchar,
	"FILE_NAME" varchar,
	"DATE_LOADED" datetime

);