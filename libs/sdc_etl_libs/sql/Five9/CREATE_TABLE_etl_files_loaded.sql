use role "AIRFLOW_SERVICE_ROLE";
use database "CONTACT_CENTER";
use schema "FIVE9";

create table "ETL_FILES_LOADED" (

    "TABLE_NAME" varchar,
	"FILE_NAME" varchar,
	"DATE_LOADED" datetime

);