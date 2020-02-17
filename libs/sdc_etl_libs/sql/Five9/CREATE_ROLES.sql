use role ACCOUNTADMIN;
use database CONTACT_CENTER;
create schema FIVE9;

--schema level roles (new roles)
create role "CONTACT_CENTER.FIVE9.pii.reader.role";
create role "CONTACT_CENTER.FIVE9.pii.writer.role";
create role "CONTACT_CENTER.FIVE9.pii.owner.role";
create role "CONTACT_CENTER.FIVE9.pii.service.role";


--table level roles

-- PHONE_CALL_DETAILS
create role "CONTACT_CENTER.FIVE9.PHONE_CALL_DETAILS.pii.table.owner";
create role "CONTACT_CENTER.FIVE9.PHONE_CALL_DETAILS.pii.table.reader";
create role "CONTACT_CENTER.FIVE9.PHONE_CALL_DETAILS.pii.table.writer";

grant ownership on table "CONTACT_CENTER"."FIVE9"."PHONE_CALL_DETAILS" to role "CONTACT_CENTER.FIVE9.PHONE_CALL_DETAILS.pii.table.owner";
grant all privileges on table "CONTACT_CENTER"."FIVE9"."PHONE_CALL_DETAILS" to role "CONTACT_CENTER.FIVE9.PHONE_CALL_DETAILS.pii.table.owner";

grant select on table "CONTACT_CENTER"."FIVE9"."PHONE_CALL_DETAILS" to role "CONTACT_CENTER.FIVE9.PHONE_CALL_DETAILS.pii.table.reader";
grant usage on database "CONTACT_CENTER" to role "CONTACT_CENTER.FIVE9.PHONE_CALL_DETAILS.pii.table.reader";
grant usage on schema "FIVE9" to role "CONTACT_CENTER.FIVE9.PHONE_CALL_DETAILS.pii.table.reader";

grant insert, update on table "CONTACT_CENTER"."FIVE9"."PHONE_CALL_DETAILS" to role "CONTACT_CENTER.FIVE9.PHONE_CALL_DETAILS.pii.table.writer";
grant usage on database "CONTACT_CENTER" to role "CONTACT_CENTER.FIVE9.PHONE_CALL_DETAILS.pii.table.writer";
grant usage on schema "FIVE9" to role "CONTACT_CENTER.FIVE9.PHONE_CALL_DETAILS.pii.table.writer";

grant role "CONTACT_CENTER.FIVE9.PHONE_CALL_DETAILS.pii.table.owner" to role "CONTACT_CENTER.FIVE9.pii.owner.role";

-- EMAIL_DETAILS
create role "CONTACT_CENTER.FIVE9.EMAIL_DETAILS.pii.table.owner";
create role "CONTACT_CENTER.FIVE9.EMAIL_DETAILS.pii.table.reader";
create role "CONTACT_CENTER.FIVE9.EMAIL_DETAILS.pii.table.writer";

grant ownership on table "CONTACT_CENTER"."FIVE9"."EMAIL_DETAILS" to role "CONTACT_CENTER.FIVE9.EMAIL_DETAILS.pii.table.owner";
grant all privileges on table "CONTACT_CENTER"."FIVE9"."EMAIL_DETAILS" to role "CONTACT_CENTER.FIVE9.EMAIL_DETAILS.pii.table.owner";

grant select on table "CONTACT_CENTER"."FIVE9"."EMAIL_DETAILS" to role "CONTACT_CENTER.FIVE9.EMAIL_DETAILS.pii.table.reader";
grant usage on database "CONTACT_CENTER" to role "CONTACT_CENTER.FIVE9.EMAIL_DETAILS.pii.table.reader";
grant usage on schema "FIVE9" to role "CONTACT_CENTER.FIVE9.EMAIL_DETAILS.pii.table.reader";

grant insert, update on table "CONTACT_CENTER"."FIVE9"."EMAIL_DETAILS" to role "CONTACT_CENTER.FIVE9.EMAIL_DETAILS.pii.table.writer";
grant usage on database "CONTACT_CENTER" to role "CONTACT_CENTER.FIVE9.EMAIL_DETAILS.pii.table.writer";
grant usage on schema "FIVE9" to role "CONTACT_CENTER.FIVE9.EMAIL_DETAILS.pii.table.writer";

grant role "CONTACT_CENTER.FIVE9.EMAIL_DETAILS.pii.table.owner" to role "CONTACT_CENTER.FIVE9.pii.owner.role";

--owner role

grant all on schema FIVE9 to role "CONTACT_CENTER.FIVE9.pii.owner.role";
grant usage on database "CONTACT_CENTER" to role "CONTACT_CENTER.FIVE9.pii.owner.role";
grant usage on schema "FIVE9" to role "CONTACT_CENTER.FIVE9.pii.owner.role";
grant role "CONTACT_CENTER.FIVE9.pii.owner.role" to role accountadmin;

--service role
grant create table, create view, create stage on schema "CONTACT_CENTER"."FIVE9" to role "CONTACT_CENTER.FIVE9.pii.service.role";
grant role "CONTACT_CENTER.FIVE9.pii.writer.role" to role "CONTACT_CENTER.FIVE9.pii.service.role";
grant role "CONTACT_CENTER.FIVE9.pii.reader.role" to role "CONTACT_CENTER.FIVE9.pii.service.role";
grant role "CONTACT_CENTER.FIVE9.pii.service.role" to role AIRFLOW_SERVICE_ROLE;

-- Reader
grant role "CONTACT_CENTER.FIVE9.PHONE_CALL_DETAILS.pii.table.reader" to role "CONTACT_CENTER.FIVE9.pii.reader.role";
grant role "CONTACT_CENTER.FIVE9.PHONE_CALL_DETAILS.pii.table.reader" to role "CONTACT_CENTER.FIVE9.pii.reader.role";

--Writer
grant role "CONTACT_CENTER.FIVE9.EMAIL_DETAILS.pii.table.writer" to role "CONTACT_CENTER.FIVE9.pii.writer.role";
grant role "CONTACT_CENTER.FIVE9.EMAIL_DETAILS.pii.table.writer" to role "CONTACT_CENTER.FIVE9.pii.writer.role";





