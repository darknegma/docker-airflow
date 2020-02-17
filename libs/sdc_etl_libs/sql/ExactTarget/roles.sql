use role ACCOUNTADMIN;
create database MARKETING;
use database MARKETING;
create schema EXACTTARGET;

--schema level roles
create role "MARKETING.EXACTTARGET.pii.reader.role";
create role "MARKETING.EXACTTARGET.pii.writer.role";
create role "MARKETING.EXACTTARGET.pii.owner.role";
create role "MARKETING.EXACTTARGET.pii.service.role";

grant usage on database MARKETING to role "MARKETING.EXACTTARGET.pii.owner.role";
grant usage on database MARKETING to role "MARKETING.EXACTTARGET.pii.writer.role";
grant usage on database MARKETING to role "MARKETING.EXACTTARGET.pii.reader.role";
grant usage on database MARKETING to role "MARKETING.EXACTTARGET.pii.service.role";

grant all on schema EXACTTARGET to role "MARKETING.EXACTTARGET.pii.owner.role";
grant usage on schema EXACTTARGET to role "MARKETING.EXACTTARGET.pii.writer.role";
grant usage on schema EXACTTARGET to role "MARKETING.EXACTTARGET.pii.reader.role";
grant usage on schema EXACTTARGET to role "MARKETING.EXACTTARGET.pii.service.role";

--Events roles
create role "MARKETING.EXACTTARGET.EVENTS_PII.pii.table.owner";
create role "MARKETING.EXACTTARGET.EVENTS_PII.pii.table.reader";
create role "MARKETING.EXACTTARGET.EVENTS_PII.pii.table.writer";

grant ownership on table "MARKETING"."EXACTTARGET"."EVENTS_PII" to role "MARKETING.EXACTTARGET.EVENTS_PII.pii.table.owner";
grant all privileges on table "MARKETING"."EXACTTARGET"."EVENTS_PII" to role "MARKETING.EXACTTARGET.EVENTS_PII.pii.table.owner";

grant select on table "MARKETING"."EXACTTARGET"."EVENTS_PII" to role "MARKETING.EXACTTARGET.EVENTS_PII.pii.table.reader";
grant usage on database "MARKETING" to role "MARKETING.EXACTTARGET.EVENTS_PII.pii.table.reader";
grant usage on schema "EXACTTARGET" to role "MARKETING.EXACTTARGET.EVENTS_PII.pii.table.reader";

grant insert,update on table "MARKETING"."EXACTTARGET"."EVENTS_PII" to role "MARKETING.EXACTTARGET.EVENTS_PII.pii.table.writer";
grant usage on database "MARKETING" to role "MARKETING.EXACTTARGET.EVENTS_PII.pii.table.writer";
grant usage on schema "EXACTTARGET" to role "MARKETING.EXACTTARGET.EVENTS_PII.pii.table.writer";

grant role "MARKETING.EXACTTARGET.EVENTS_PII.pii.table.owner" to role "MARKETING.EXACTTARGET.pii.owner.role";


--Subscriber roles
create role "MARKETING.EXACTTARGET.SUBSCRIBERS_PII.pii.table.owner";
create role "MARKETING.EXACTTARGET.SUBSCRIBERS_PII.pii.table.reader";
create role "MARKETING.EXACTTARGET.SUBSCRIBERS_PII.pii.table.writer";

grant ownership on table "MARKETING"."EXACTTARGET"."SUBSCRIBERS_PII" to role "MARKETING.EXACTTARGET.SUBSCRIBERS_PII.pii.table.owner";
grant all privileges on table "MARKETING"."EXACTTARGET"."SUBSCRIBERS_PII" to role "MARKETING.EXACTTARGET.SUBSCRIBERS_PII.pii.table.owner";

grant select on table "MARKETING"."EXACTTARGET"."SUBSCRIBERS_PII" to role "MARKETING.EXACTTARGET.SUBSCRIBERS_PII.pii.table.reader";
grant usage on database "MARKETING" to role "MARKETING.EXACTTARGET.SUBSCRIBERS_PII.pii.table.reader";
grant usage on schema "EXACTTARGET" to role "MARKETING.EXACTTARGET.SUBSCRIBERS_PII.pii.table.reader";

grant insert,update on table "MARKETING"."EXACTTARGET"."SUBSCRIBERS_PII" to role "MARKETING.EXACTTARGET.SUBSCRIBERS_PII.pii.table.writer";
grant usage on database "MARKETING" to role "MARKETING.EXACTTARGET.SUBSCRIBERS_PII.pii.table.writer";
grant usage on schema "EXACTTARGET" to role "MARKETING.EXACTTARGET.SUBSCRIBERS_PII.pii.table.writer";

grant role "MARKETING.EXACTTARGET.SUBSCRIBERS_PII.pii.table.owner" to role "MARKETING.EXACTTARGET.pii.owner.role";


--List Subscriber roles
create role "MARKETING.EXACTTARGET.LIST_SUBSCRIBERS_PII.pii.table.owner";
create role "MARKETING.EXACTTARGET.LIST_SUBSCRIBERS_PII.pii.table.reader";
create role "MARKETING.EXACTTARGET.LIST_SUBSCRIBERS_PII.pii.table.writer";

grant ownership on table "MARKETING"."EXACTTARGET"."LIST_SUBSCRIBERS_PII" to role "MARKETING.EXACTTARGET.LIST_SUBSCRIBERS_PII.pii.table.owner";
grant all privileges on table "MARKETING"."EXACTTARGET"."LIST_SUBSCRIBERS_PII" to role "MARKETING.EXACTTARGET.LIST_SUBSCRIBERS_PII.pii.table.owner";

grant select on table "MARKETING"."EXACTTARGET"."LIST_SUBSCRIBERS" to role "MARKETING.EXACTTARGET.LIST_SUBSCRIBERS_PII.pii.table.reader";
grant usage on database "MARKETING" to role "MARKETING.EXACTTARGET.LIST_SUBSCRIBERS_PII.pii.table.reader";
grant usage on schema "EXACTTARGET" to role "MARKETING.EXACTTARGET.LIST_SUBSCRIBERS_PII.pii.table.reader";

grant insert,update on table "MARKETING"."EXACTTARGET"."LIST_SUBSCRIBERS" to role "MARKETING.EXACTTARGET.LIST_SUBSCRIBERS_PII.pii.table.writer";
grant usage on database "MARKETING" to role "MARKETING.EXACTTARGET.LIST_SUBSCRIBERS_PII.pii.table.writer";
grant usage on schema "EXACTTARGET" to role "MARKETING.EXACTTARGET.LIST_SUBSCRIBERS_PII.pii.table.writer";

grant role "MARKETING.EXACTTARGET.LIST_SUBSCRIBERS_PII.pii.table.owner" to role "MARKETING.EXACTTARGET.pii.owner.role";


--Send roles
create role "MARKETING.EXACTTARGET.SENDS_PII.pii.table.owner";
create role "MARKETING.EXACTTARGET.SENDS_PII.pii.table.reader";
create role "MARKETING.EXACTTARGET.SENDS_PII.pii.table.writer";

grant ownership on table "MARKETING"."EXACTTARGET"."SENDS_PII" to role "MARKETING.EXACTTARGET.SENDS_PII.pii.table.owner";
grant all privileges on table "MARKETING"."EXACTTARGET"."SENDS_PII" to role "MARKETING.EXACTTARGET.SENDS_PII.pii.table.owner";

grant select on table "MARKETING"."EXACTTARGET"."SENDS_PII" to role "MARKETING.EXACTTARGET.SENDS_PII.pii.table.reader";
grant usage on database "MARKETING" to role "MARKETING.EXACTTARGET.SENDS_PII.pii.table.reader";
grant usage on schema "EXACTTARGET" to role "MARKETING.EXACTTARGET.SENDS_PII.pii.table.reader";

grant insert,update on table "MARKETING"."EXACTTARGET"."SENDS_PII" to role "MARKETING.EXACTTARGET.SENDS_PII.pii.table.writer";
grant usage on database "MARKETING" to role "MARKETING.EXACTTARGET.SENDS_PII.pii.table.writer";
grant usage on schema "EXACTTARGET" to role "MARKETING.EXACTTARGET.SENDS_PII.pii.table.writer";

grant role "MARKETING.EXACTTARGET.SENDS_PII.pii.table.owner" to role "MARKETING.EXACTTARGET.pii.owner.role";


--Reader row
grant role "MARKETING.EXACTTARGET.SENDS_PII.pii.table.reader" to role "MARKETING.EXACTTARGET.pii.reader.role";
grant role "MARKETING.EXACTTARGET.LIST_SUBSCRIBERS_PII.pii.table.reader" to role "MARKETING.EXACTTARGET.pii.reader.role";
grant role "MARKETING.EXACTTARGET.SUBSCRIBERS_PII.pii.table.reader" to role "MARKETING.EXACTTARGET.pii.reader.role";
grant role "MARKETING.EXACTTARGET.EVENTS_PII.pii.table.reader" to role "MARKETING.EXACTTARGET.pii.reader.role";

--Writer
grant usage, create table, create view, create stage on schema "MARKETING"."EXACTTARGET" to role "MARKETING.EXACTTARGET.pii.writer.role";
grant role "MARKETING.EXACTTARGET.SENDS_PII.pii.table.writer" to role "MARKETING.EXACTTARGET.pii.writer.role";
grant role "MARKETING.EXACTTARGET.LIST_SUBSCRIBERS_PII.pii.table.writer" to role "MARKETING.EXACTTARGET.pii.writer.role";
grant role "MARKETING.EXACTTARGET.SUBSCRIBERS_PII.pii.table.writer" to role "MARKETING.EXACTTARGET.pii.writer.role";
grant role "MARKETING.EXACTTARGET.EVENTS_PII.pii.table.writer" to role "MARKETING.EXACTTARGET.pii.writer.role";

--service role

grant role "MARKETING.EXACTTARGET.pii.writer.role" to role "MARKETING.EXACTTARGET.pii.service.role";
grant role "MARKETING.EXACTTARGET.pii.reader.role" to role "MARKETING.EXACTTARGET.pii.service.role";


grant role "MARKETING.EXACTTARGET.pii.service.role" to role "AIRFLOW_SERVICE_ROLE";