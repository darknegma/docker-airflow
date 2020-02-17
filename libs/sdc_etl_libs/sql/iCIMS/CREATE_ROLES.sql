
use role SECURITYADMIN;
use database HRIS_DATA;

create role "TRANSFORMING_WAREHOUSE.HRIS_DATA.ICIMS.pii.reader.role";
create role "AIRFLOW_WAREHOUSE.HRIS_DATA.ICIMS.pii.writer.role";
create role "AIRFLOW_WAREHOUSE.HRIS_DATA.ICIMS.pii.owner.role";
create role "AIRFLOW_WAREHOUSE.HRIS_DATA.ICIMS.pii.service.role";

grant role "AIRFLOW_WAREHOUSE.HRIS_DATA.ICIMS.pii.writer.role" to role "AIRFLOW_WAREHOUSE.HRIS_DATA.ICIMS.pii.service.role";
grant role "AIRFLOW_WAREHOUSE.HRIS_DATA.ICIMS.pii.service.role" to user AIRFLOW;
grant usage on warehouse AIRFLOW_WAREHOUSE to role "AIRFLOW_WAREHOUSE.HRIS_DATA.ICIMS.pii.owner.role";
grant usage on warehouse AIRFLOW_WAREHOUSE to role "AIRFLOW_WAREHOUSE.HRIS_DATA.ICIMS.pii.service.role";

grant usage on database "HRIS_DATA" to role "TRANSFORMING_WAREHOUSE.HRIS_DATA.ICIMS.pii.reader.role";
grant usage on database "HRIS_DATA" to role "AIRFLOW_WAREHOUSE.HRIS_DATA.ICIMS.pii.writer.role";
grant usage on database "HRIS_DATA" to role "AIRFLOW_WAREHOUSE.HRIS_DATA.ICIMS.pii.owner.role";
grant usage on database "HRIS_DATA" to role "AIRFLOW_WAREHOUSE.HRIS_DATA.ICIMS.pii.service.role";

grant create schema on database "HRIS_DATA" to role "AIRFLOW_WAREHOUSE.HRIS_DATA.ICIMS.pii.owner.role";
use role "AIRFLOW_WAREHOUSE.HRIS_DATA.ICIMS.pii.owner.role";
use database HRIS_DATA;
create schema ICIMS;
use schema ICIMS;

grant usage on schema "HRIS_DATA"."ICIMS" to role "AIRFLOW_WAREHOUSE.HRIS_DATA.ICIMS.pii.service.role";
grant create table, create view, create stage on schema "HRIS_DATA"."ICIMS" to role "AIRFLOW_WAREHOUSE.HRIS_DATA.ICIMS.pii.service.role"

grant usage on schema "HRIS_DATA"."ICIMS" to role "AIRFLOW_WAREHOUSE.HRIS_DATA.ICIMS.pii.writer.role";
grant insert, update on all tables in schema "HRIS_DATA"."ICIMS" to role "AIRFLOW_WAREHOUSE.HRIS_DATA.ICIMS.pii.writer.role";

grant usage on schema "HRIS_DATA"."ICIMS" to role "TRANSFORMING_WAREHOUSE.HRIS_DATA.ICIMS.pii.reader.role";
grant select on all tables in schema "HRIS_DATA"."ICIMS" to role "TRANSFORMING_WAREHOUSE.HRIS_DATA.ICIMS.pii.reader.role";