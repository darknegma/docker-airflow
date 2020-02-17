use role ACCOUNTADMIN;
use database MARKETING;
create schema NEUSTAR

CREATE ROLE "MARKETING.NEUSTAR.no_pii.reader.role";
CREATE ROLE "MARKETING.NEUSTAR.no_pii.writer.role";
CREATE ROLE "MARKETING.NEUSTAR.no_pii.owner.role";
CREATE ROLE "MARKETING.NEUSTAR.no_pii.service.role";

grant usage on database "MARKETING" to role "MARKETING.NEUSTAR.no_pii.owner.role";
grant usage on database "MARKETING" to role "MARKETING.NEUSTAR.no_pii.reader.role";
grant usage on database "MARKETING" to role "MARKETING.NEUSTAR.no_pii.writer.role";
grant usage on database "MARKETING" to role "MARKETING.NEUSTAR.no_pii.service.role";

grant usage on warehouse AIRFLOW_WAREHOUSE to role "MARKETING.NEUSTAR.no_pii.owner.role";
grant usage on warehouse AIRFLOW_WAREHOUSE to role "MARKETING.NEUSTAR.no_pii.service.role";
grant role "MARKETING.NEUSTAR.no_pii.writer.role" to role "MARKETING.NEUSTAR.no_pii.service.role";
grant role "MARKETING.NEUSTAR.no_pii.service.role" to role "AIRFLOW_SERVICE_ROLE";

grant usage on schema "MARKETING"."NEUSTAR" to role "MARKETING.NEUSTAR.no_pii.owner.role";
grant create table, create view, create stage on schema "MARKETING"."NEUSTAR" to role "MARKETING.NEUSTAR.no_pii.owner.role";
grant usage on schema "MARKETING"."NEUSTAR" to role "MARKETING.NEUSTAR.no_pii.service.role";
grant create table, create view, create stage on schema "MARKETING"."NEUSTAR" to role "MARKETING.NEUSTAR.no_pii.service.role";

grant usage on schema "MARKETING"."NEUSTAR" to role "MARKETING.NEUSTAR.no_pii.writer.role";
grant usage on schema "MARKETING"."NEUSTAR" to role "MARKETING.NEUSTAR.no_pii.reader.role";