use role ACCOUNTADMIN;
use database LOGISTICS;
create schema TNT;

--schema level roles (new roles)
create role "LOGISTICS.TNT.pii.reader.role";
create role "LOGISTICS.TNT.pii.writer.role";
create role "LOGISTICS.TNT.pii.owner.role";
create role "LOGISTICS.TNT.pii.service.role";


--table level roles

-- TRACKING_EVENTS
create role "LOGISTICS.TNT.TRACKING_EVENTS.pii.table.owner";
create role "LOGISTICS.TNT.TRACKING_EVENTS.pii.table.reader";
create role "LOGISTICS.TNT.TRACKING_EVENTS.pii.table.writer";

grant ownership on table "LOGISTICS"."TNT"."TRACKING_EVENTS" to role "LOGISTICS.TNT.TRACKING_EVENTS.pii.table.owner";
grant all privileges on table "LOGISTICS"."TNT"."TRACKING_EVENTS" to role "LOGISTICS.TNT.TRACKING_EVENTS.pii.table.owner";

grant select on table "LOGISTICS"."TNT"."TRACKING_EVENTS" to role "LOGISTICS.TNT.TRACKING_EVENTS.pii.table.reader";
grant usage on database "LOGISTICS" to role "LOGISTICS.TNT.TRACKING_EVENTS.pii.table.reader";
grant usage on schema "TNT" to role "LOGISTICS.TNT.TRACKING_EVENTS.pii.table.reader";

grant insert,update on table "LOGISTICS"."TNT"."TRACKING_EVENTS" to role "LOGISTICS.TNT.TRACKING_EVENTS.pii.table.writer";
grant usage on database "LOGISTICS" to role "LOGISTICS.TNT.TRACKING_EVENTS.pii.table.writer";
grant usage on schema "TNT" to role "LOGISTICS.TNT.TRACKING_EVENTS.pii.table.writer";

grant role "LOGISTICS.TNT.TRACKING_EVENTS.pii.table.owner" to role "LOGISTICS.TNT.pii.owner.role";


--owner role
grant all on schema TNT to role "LOGISTICS.TNT.pii.owner.role";
grant usage on database "LOGISTICS" to role "LOGISTICS.TNT.pii.owner.role";
grant usage on schema "TNT" to role "LOGISTICS.TNT.pii.owner.role";
grant role "LOGISTICS.TNT.pii.owner.role" to role accountadmin;

--service role
grant create table, create view, create stage on schema "LOGISTICS"."TNT" to role "LOGISTICS.TNT.pii.service.role";
grant role "LOGISTICS.TNT.pii.writer.role" to role "LOGISTICS.TNT.pii.service.role";
grant role "LOGISTICS.TNT.pii.reader.role" to role "LOGISTICS.TNT.pii.service.role";
grant role "LOGISTICS.TNT.pii.service.role" to role AIRFLOW_SERVICE_ROLE;

-- Reader
grant role "LOGISTICS.TNT.TRACKING_EVENTS.pii.table.reader" to role "LOGISTICS.TNT.pii.reader.role";

--Writer
grant role "LOGISTICS.TNT.TRACKING_EVENTS.pii.table.writer" to role "LOGISTICS.TNT.pii.writer.role";


-- Views
-- VW_TRACKING_EVENTS
create role "LOGISTICS.TNT.VW_TRACKING_EVENTS.no_pii.view.reader";
grant usage on database "LOGISTICS" to role "LOGISTICS.TNT.VW_TRACKING_EVENTS.no_pii.view.reader";
grant usage on schema "TNT" to role "LOGISTICS.TNT.VW_TRACKING_EVENTS.no_pii.view.reader";
grant select on "LOGISTICS"."TNT"."VW_TRACKING_EVENTS" to role "LOGISTICS.TNT.VW_TRACKING_EVENTS.no_pii.view.reader";
grant role "LOGISTICS.TNT.VW_TRACKING_EVENTS.no_pii.view.reader" to role TRANSFORMER;
