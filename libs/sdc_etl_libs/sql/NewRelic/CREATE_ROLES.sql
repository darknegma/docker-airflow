
use role SECURITYADMIN;
use database WEB_DATA;
create schema NEW_RELIC;
use schema NEW_RELIC;

create role "WEB_DATA.NEW_RELIC.no_pii.reader.role";
create role "WEB_DATA.NEW_RELIC.no_pii.writer.role";
create role "WEB_DATA.NEW_RELIC.no_pii.owner.role";
create role "WEB_DATA.NEW_RELIC.no_pii.service.role";

grant usage on warehouse AIRFLOW_WAREHOUSE to role "WEB_DATA.NEW_RELIC.no_pii.owner.role";
grant usage on warehouse AIRFLOW_WAREHOUSE to role "WEB_DATA.NEW_RELIC.no_pii.service.role";

grant usage on database "WEB_DATA" to role "WEB_DATA.NEW_RELIC.no_pii.owner.role";
grant usage on database "WEB_DATA" to role "WEB_DATA.NEW_RELIC.no_pii.reader.role";
grant usage on database "WEB_DATA" to role "WEB_DATA.NEW_RELIC.no_pii.writer.role";
grant usage on database "WEB_DATA" to role "WEB_DATA.NEW_RELIC.no_pii.service.role";

grant usage on schema "WEB_DATA"."NEW_RELIC"to role "WEB_DATA.NEW_RELIC.no_pii.owner.role";
grant usage on schema "WEB_DATA"."NEW_RELIC" to role "WEB_DATA.NEW_RELIC.no_pii.reader.role";
grant usage on schema "WEB_DATA"."NEW_RELIC"to role "WEB_DATA.NEW_RELIC.no_pii.writer.role";
grant usage on schema "WEB_DATA"."NEW_RELIC" to role "WEB_DATA.NEW_RELIC.no_pii.service.role";

-- Owner role
grant ownership on all tables in schema "WEB_DATA"."NEW_RELIC" to role "WEB_DATA.NEW_RELIC.no_pii.owner.role";

-- Writer role
grant insert, update on all tables in schema "WEB_DATA"."NEW_RELIC" to role "WEB_DATA.NEW_RELIC.no_pii.writer.role";

-- Reader role
grant select on all tables in schema "WEB_DATA"."NEW_RELIC" to role "WEB_DATA.NEW_RELIC.no_pii.reader.role";

-- Service role
grant create table, create view, create stage on schema "WEB_DATA"."NEW_RELIC" to role "WEB_DATA.NEW_RELIC.no_pii.service.role";
grant role "WEB_DATA.NEW_RELIC.no_pii.writer.role" to role "WEB_DATA.NEW_RELIC.no_pii.service.role";
grant role "WEB_DATA.NEW_RELIC.no_pii.reader.role" to role "WEB_DATA.NEW_RELIC.no_pii.service.role";
grant role "WEB_DATA.NEW_RELIC.no_pii.service.role" to role AIRFLOW_SERVICE_ROLE;
