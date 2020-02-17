use role "AIRFLOW_SERVICE_ROLE";
use database "LOGISTICS";
use schema "BARRETT";

create table "ETL_FILES_LOADED" (

    "TABLE_NAME" varchar,
	"FILE_NAME" varchar,
	"DATE_LOADED" datetime

);