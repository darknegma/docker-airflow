
use role SECURITYADMIN;

create role "LOGISTICS.BARRETT.no_pii.reader.role";
create role "LOGISTICS.BARRETT.no_pii.writer.role";
create role "LOGISTICS.BARRETT.no_pii.owner.role";
create role "LOGISTICS.BARRETT.no_pii.service.role";

grant role "LOGISTICS.BARRETT.no_pii.writer.role" to role "LOGISTICS.BARRETT.no_pii.service.role";
grant role "LOGISTICS.BARRETT.no_pii.service.role" to user AIRFLOW;
grant usage on warehouse AIRFLOW_WAREHOUSE to role "LOGISTICS.BARRETT.no_pii.owner.role";
grant usage on warehouse AIRFLOW_WAREHOUSE to role "LOGISTICS.BARRETT.no_pii.service.role";

grant usage on database "LOGISTICS" to role "LOGISTICS.BARRETT.no_pii.owner.role";
grant usage on database "LOGISTICS" to role "LOGISTICS.BARRETT.no_pii.reader.role";
grant usage on database "LOGISTICS" to role "LOGISTICS.BARRETT.no_pii.writer.role";
grant usage on database "LOGISTICS" to role "LOGISTICS.BARRETT.no_pii.service.role";

grant create schema on database "LOGISTICS" to role "LOGISTICS.BARRETT.no_pii.owner.role";
use role "LOGISTICS.BARRETT.no_pii.owner.role";
use database LOGISTICS;
create schema BARRETT;

grant usage on schema "LOGISTICS"."BARRETT" to role "LOGISTICS.BARRETT.no_pii.service.role";
grant create table, create view, create stage on schema "LOGISTICS"."BARRETT" to role "LOGISTICS.BARRETT.no_pii.service.role";

grant usage on schema "LOGISTICS"."BARRETT" to role "LOGISTICS.BARRETT.no_pii.writer.role";
grant insert, update on all tables in schema "LOGISTICS"."BARRETT" to role "LOGISTICS.BARRETT.no_pii.writer.role";

grant usage on schema "LOGISTICS"."BARRETT" to role "LOGISTICS.BARRETT.no_pii.reader.role";