use role "AIRFLOW_SERVICE_ROLE";
use database "MARKETING";
use schema "DATAXU";

create table "ETL_FILES_LOADED" (

    "TABLE_NAME" varchar,
	"FILE_NAME" varchar,
	"DATE_LOADED" datetime

);