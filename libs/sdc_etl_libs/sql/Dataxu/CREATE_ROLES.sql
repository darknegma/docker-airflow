use role ACCOUNTADMIN;
use database MARKETING;
create schema DATAXU;

--schema level roles (new roles)
create role "MARKETING.DATAXU.no_pii.reader.role";
create role "MARKETING.DATAXU.no_pii.writer.role";
create role "MARKETING.DATAXU.no_pii.owner.role";
create role "MARKETING.DATAXU.no_pii.service.role";


--table level roles

-- DAILY_IMPRESSIONS
create role "MARKETING.DATAXU.DAILY_IMPRESSIONS.no_pii.table.owner";
create role "MARKETING.DATAXU.DAILY_IMPRESSIONS.no_pii.table.reader";
create role "MARKETING.DATAXU.DAILY_IMPRESSIONS.no_pii.table.writer";

grant ownership on table "MARKETING"."DATAXU"."DAILY_IMPRESSIONS" to role "MARKETING.DATAXU.DAILY_IMPRESSIONS.no_pii.table.owner";
grant all privileges on table "MARKETING"."DATAXU"."DAILY_IMPRESSIONS" to role "MARKETING.DATAXU.DAILY_IMPRESSIONS.no_pii.table.owner";

grant select on table "MARKETING"."DATAXU"."DAILY_IMPRESSIONS" to role "MARKETING.DATAXU.DAILY_IMPRESSIONS.no_pii.table.reader";
grant usage on database "MARKETING" to role "MARKETING.DATAXU.DAILY_IMPRESSIONS.no_pii.table.reader";
grant usage on schema "DATAXU" to role "MARKETING.DATAXU.DAILY_IMPRESSIONS.no_pii.table.reader";

grant insert, update on table "MARKETING"."DATAXU"."DAILY_IMPRESSIONS" to role "MARKETING.DATAXU.DAILY_IMPRESSIONS.no_pii.table.writer";
grant usage on database "MARKETING" to role "MARKETING.DATAXU.DAILY_IMPRESSIONS.no_pii.table.writer";
grant usage on schema "DATAXU" to role "MARKETING.DATAXU.DAILY_IMPRESSIONS.no_pii.table.writer";

grant role "MARKETING.DATAXU.DAILY_IMPRESSIONS.no_pii.table.owner" to role "MARKETING.DATAXU.no_pii.owner.role";

--owner role

grant all on schema DATAXU to role "MARKETING.DATAXU.no_pii.owner.role";
grant usage on database "MARKETING" to role "MARKETING.DATAXU.no_pii.owner.role";
grant usage on schema "DATAXU" to role "MARKETING.DATAXU.no_pii.owner.role";
grant role "MARKETING.DATAXU.no_pii.owner.role" to role accountadmin;

--service role
grant create table, create view, create stage on schema "MARKETING"."DATAXU" to role "MARKETING.DATAXU.no_pii.service.role";
grant role "MARKETING.DATAXU.no_pii.writer.role" to role "MARKETING.DATAXU.no_pii.service.role";
grant role "MARKETING.DATAXU.no_pii.reader.role" to role "MARKETING.DATAXU.no_pii.service.role";
grant role "MARKETING.DATAXU.no_pii.service.role" to role AIRFLOW_SERVICE_ROLE;

-- Reader
grant role "MARKETING.DATAXU.DAILY_IMPRESSIONS.no_pii.table.reader" to role "MARKETING.DATAXU.no_pii.reader.role";

--Writer
grant role "MARKETING.DATAXU.DAILY_IMPRESSIONS.no_pii.table.writer" to role "MARKETING.DATAXU.no_pii.writer.role";

--DBT No PII access
grant role "MARKETING.DATAXU.no_pii.reader.role"  to role "DBT_DEV";






