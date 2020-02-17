
use role ACCOUNTADMIN;
use database MARKETING;
create schema PODIUM;

CREATE ROLE "MARKETING.PODIUM.no_pii.reader.role";
CREATE ROLE "MARKETING.PODIUM.no_pii.writer.role";
CREATE ROLE "MARKETING.PODIUM.no_pii.owner.role";
CREATE ROLE "MARKETING.PODIUM.no_pii.service.role";

grant role "MARKETING.PODIUM.no_pii.writer.role" to role "MARKETING.PODIUM.no_pii.service.role";
grant role "MARKETING.PODIUM.no_pii.service.role" to user AIRFLOW;
grant usage on warehouse AIRFLOW_WAREHOUSE to role "MARKETING.PODIUM.no_pii.owner.role";
grant usage on warehouse AIRFLOW_WAREHOUSE to role "MARKETING.PODIUM.no_pii.service.role";

grant usage on database "MARKETING" to role "MARKETING.PODIUM.no_pii.owner.role";
grant usage on database "MARKETING" to role "MARKETING.PODIUM.no_pii.reader.role";
grant usage on database "MARKETING" to role "MARKETING.PODIUM.no_pii.writer.role";
grant usage on database "MARKETING" to role "MARKETING.PODIUM.no_pii.service.role";

grant usage on schema "MARKETING"."PODIUM" to role "MARKETING.PODIUM.no_pii.owner.role";
grant create table, create view, create stage on schema "MARKETING"."PODIUM" to role "MARKETING.PODIUM.no_pii.owner.role";
grant usage on schema "MARKETING"."PODIUM" to role "MARKETING.PODIUM.no_pii.service.role";
grant create table, create view, create stage on schema "MARKETING"."PODIUM" to role "MARKETING.PODIUM.no_pii.service.role";

grant usage on schema "MARKETING"."PODIUM" to role "MARKETING.PODIUM.no_pii.writer.role";
grant insert, update on all tables in schema "MARKETING"."PODIUM" to role "MARKETING.PODIUM.no_pii.writer.role";

grant usage on schema "MARKETING"."PODIUM" to role "MARKETING.PODIUM.no_pii.reader.role";



CREATE ROLE "MARKETING.PODIUM.pii.reader.role";
CREATE ROLE "MARKETING.PODIUM.pii.writer.role";
CREATE ROLE "MARKETING.PODIUM.pii.owner.role";
CREATE ROLE "MARKETING.PODIUM.pii.service.role";

grant role "MARKETING.PODIUM.pii.writer.role" to role "MARKETING.PODIUM.pii.service.role";
grant role "MARKETING.PODIUM.pii.service.role" to user AIRFLOW;
grant usage on warehouse AIRFLOW_WAREHOUSE to role "MARKETING.PODIUM.pii.owner.role";
grant usage on warehouse AIRFLOW_WAREHOUSE to role "MARKETING.PODIUM.pii.service.role";

grant usage on database "MARKETING" to role "MARKETING.PODIUM.pii.owner.role";
grant usage on database "MARKETING" to role "MARKETING.PODIUM.pii.reader.role";
grant usage on database "MARKETING" to role "MARKETING.PODIUM.pii.writer.role";
grant usage on database "MARKETING" to role "MARKETING.PODIUM.pii.service.role";

grant usage on schema "MARKETING"."PODIUM" to role "MARKETING.PODIUM.pii.owner.role";
grant create table, create view, create stage on schema "MARKETING"."PODIUM" to role "MARKETING.PODIUM.pii.owner.role";
grant usage on schema "MARKETING"."PODIUM" to role "MARKETING.PODIUM.pii.service.role";
grant create table, create view, create stage on schema "MARKETING"."PODIUM" to role "MARKETING.PODIUM.pii.service.role";

grant usage on schema "MARKETING"."PODIUM" to role "MARKETING.PODIUM.pii.writer.role";
grant insert, update on all tables in schema "MARKETING"."PODIUM" to role "MARKETING.PODIUM.pii.writer.role";

grant usage on schema "MARKETING"."PODIUM" to role "MARKETING.PODIUM.pii.reader.role";









