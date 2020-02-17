use role "MARKETING.NEUSTAR.no_pii.owner.role";
use database "MARKETING";
use schema "NEUSTAR";

CREATE OR REPLACE TABLE "etl_files_loaded"(
	FILE_NAME varchar,
	DATE_LOADED datetime,
    FILE_ID INTEGER,
    NUMBER_OF_ROWS INTEGER,
    LOAD_STATUS STRING
);


grant SELECT, insert, update on table "MARKETING"."NEUSTAR"."etl_files_loaded" to role "MARKETING.NEUSTAR.no_pii.writer.role";
grant select on table "MARKETING"."NEUSTAR"."etl_files_loaded" to role "MARKETING.NEUSTAR.no_pii.reader.role";

grant role "MARKETING.NEUSTAR.no_pii.reader.role" to role TRANSFORMER;


