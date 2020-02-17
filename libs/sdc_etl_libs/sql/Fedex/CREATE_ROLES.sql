use role ACCOUNTADMIN;
use database LOGISTICS;
create schema FEDEX;

--schema level roles (new roles)
create role "LOGISTICS.FEDEX.pii.reader.role";
create role "LOGISTICS.FEDEX.pii.writer.role";
create role "LOGISTICS.FEDEX.pii.owner.role";
create role "LOGISTICS.FEDEX.pii.service.role";


--table level roles

-- TRACKING_EVENTS
create role "LOGISTICS.FEDEX.TRACKING_EVENTS.pii.table.owner";
create role "LOGISTICS.FEDEX.TRACKING_EVENTS.pii.table.reader";
create role "LOGISTICS.FEDEX.TRACKING_EVENTS.pii.table.writer";

grant ownership on table "LOGISTICS"."FEDEX"."TRACKING_EVENTS" to role "LOGISTICS.FEDEX.TRACKING_EVENTS.pii.table.owner";
grant all privileges on table "LOGISTICS"."FEDEX"."TRACKING_EVENTS" to role "LOGISTICS.FEDEX.TRACKING_EVENTS.pii.table.owner";

grant select on table "LOGISTICS"."FEDEX"."TRACKING_EVENTS" to role "LOGISTICS.FEDEX.TRACKING_EVENTS.pii.table.reader";
grant usage on database "LOGISTICS" to role "LOGISTICS.FEDEX.TRACKING_EVENTS.pii.table.reader";
grant usage on schema "FEDEX" to role "LOGISTICS.FEDEX.TRACKING_EVENTS.pii.table.reader";

grant insert,update on table "LOGISTICS"."FEDEX"."TRACKING_EVENTS" to role "LOGISTICS.FEDEX.TRACKING_EVENTS.pii.table.writer";
grant usage on database "LOGISTICS" to role "LOGISTICS.FEDEX.TRACKING_EVENTS.pii.table.writer";
grant usage on schema "FEDEX" to role "LOGISTICS.FEDEX.TRACKING_EVENTS.pii.table.writer";

grant role "LOGISTICS.FEDEX.TRACKING_EVENTS.pii.table.owner" to role "LOGISTICS.FEDEX.pii.owner.role";


--owner role
grant all on schema FEDEX to role "LOGISTICS.FEDEX.pii.owner.role";
grant usage on database "LOGISTICS" to role "LOGISTICS.FEDEX.pii.owner.role";
grant usage on schema "FEDEX" to role "LOGISTICS.FEDEX.pii.owner.role";
grant role "LOGISTICS.FEDEX.pii.owner.role" to role accountadmin;

--service role
grant create table, create view, create stage on schema "LOGISTICS"."FEDEX" to role "LOGISTICS.FEDEX.pii.service.role";
grant role "LOGISTICS.FEDEX.pii.writer.role" to role "LOGISTICS.FEDEX.pii.service.role";
grant role "LOGISTICS.FEDEX.pii.reader.role" to role "LOGISTICS.FEDEX.pii.service.role";
grant role "LOGISTICS.FEDEX.pii.service.role" to role AIRFLOW_SERVICE_ROLE;

-- Reader
grant role "LOGISTICS.FEDEX.TRACKING_EVENTS.pii.table.reader" to role "LOGISTICS.FEDEX.pii.reader.role";

--Writer
grant role "LOGISTICS.FEDEX.TRACKING_EVENTS.pii.table.writer" to role "LOGISTICS.FEDEX.pii.writer.role";


-- Views
-- VW_TRACKING_EVENTS
create role "LOGISTICS.FEDEX.VW_TRACKING_EVENTS.no_pii.view.reader";
grant usage on database "LOGISTICS" to role "LOGISTICS.FEDEX.VW_TRACKING_EVENTS.no_pii.view.reader";
grant usage on schema "FEDEX" to role "LOGISTICS.FEDEX.VW_TRACKING_EVENTS.no_pii.view.reader";
grant select on "LOGISTICS"."FEDEX"."VW_TRACKING_EVENTS" to role "LOGISTICS.FEDEX.VW_TRACKING_EVENTS.no_pii.view.reader";
grant role "LOGISTICS.FEDEX.VW_TRACKING_EVENTS.no_pii.view.reader" to role TRANSFORMER;
