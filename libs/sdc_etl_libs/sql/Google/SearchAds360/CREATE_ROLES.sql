use role ACCOUNTADMIN;
use database MARKETING;
create schema GOOGLE_SEARCH_ADS_360;

--schema level roles (new roles)
create role "MARKETING.GOOGLE_SEARCH_ADS_360.no_pii.reader.role";
create role "MARKETING.GOOGLE_SEARCH_ADS_360.no_pii.writer.role";
create role "MARKETING.GOOGLE_SEARCH_ADS_360.no_pii.owner.role";
create role "MARKETING.GOOGLE_SEARCH_ADS_360.no_pii.service.role";


--table level roles

-- REPORT_CONVERSION_EVENTS
create role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_CONVERSION_EVENTS.no_pii.table.owner";
create role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_CONVERSION_EVENTS.no_pii.table.reader";
create role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_CONVERSION_EVENTS.no_pii.table.writer";

grant ownership on table "MARKETING"."GOOGLE_SEARCH_ADS_360"."REPORT_CONVERSION_EVENTS" to role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_CONVERSION_EVENTS.no_pii.table.owner";
grant all privileges on table "MARKETING"."GOOGLE_SEARCH_ADS_360"."REPORT_CONVERSION_EVENTS" to role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_CONVERSION_EVENTS.no_pii.table.owner";

grant select on table "MARKETING"."GOOGLE_SEARCH_ADS_360"."REPORT_CONVERSION_EVENTS" to role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_CONVERSION_EVENTS.no_pii.table.reader";
grant usage on database "MARKETING" to role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_CONVERSION_EVENTS.no_pii.table.reader";
grant usage on schema "GOOGLE_SEARCH_ADS_360" to role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_CONVERSION_EVENTS.no_pii.table.reader";

grant insert,update on table "MARKETING"."GOOGLE_SEARCH_ADS_360"."REPORT_CONVERSION_EVENTS" to role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_CONVERSION_EVENTS.no_pii.table.writer";
grant usage on database "MARKETING" to role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_CONVERSION_EVENTS.no_pii.table.writer";
grant usage on schema "GOOGLE_SEARCH_ADS_360" to role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_CONVERSION_EVENTS.no_pii.table.writer";

grant role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_CONVERSION_EVENTS.no_pii.table.owner" to role "MARKETING.GOOGLE_SEARCH_ADS_360.no_pii.owner.role";

-- REPORT_ADS
create role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_ADS.no_pii.table.owner";
create role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_ADS.no_pii.table.reader";
create role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_ADS.no_pii.table.writer";

grant ownership on table "MARKETING"."GOOGLE_SEARCH_ADS_360"."REPORT_ADS" to role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_ADS.no_pii.table.owner";
grant all privileges on table "MARKETING"."GOOGLE_SEARCH_ADS_360"."REPORT_ADS" to role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_ADS.no_pii.table.owner";

grant select on table "MARKETING"."GOOGLE_SEARCH_ADS_360"."REPORT_ADS" to role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_ADS.no_pii.table.reader";
grant usage on database "MARKETING" to role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_ADS.no_pii.table.reader";
grant usage on schema "GOOGLE_SEARCH_ADS_360" to role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_ADS.no_pii.table.reader";

grant insert,update on table "MARKETING"."GOOGLE_SEARCH_ADS_360"."REPORT_ADS" to role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_ADS.no_pii.table.writer";
grant usage on database "MARKETING" to role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_ADS.no_pii.table.writer";
grant usage on schema "GOOGLE_SEARCH_ADS_360" to role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_ADS.no_pii.table.writer";

grant role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_ADS.no_pii.table.owner" to role "MARKETING.GOOGLE_SEARCH_ADS_360.no_pii.owner.role";

-- REPORT_KEYWORDS
create role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_KEYWORDS.no_pii.table.owner";
create role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_KEYWORDS.no_pii.table.reader";
create role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_KEYWORDS.no_pii.table.writer";

grant ownership on table "MARKETING"."GOOGLE_SEARCH_ADS_360"."REPORT_KEYWORDS" to role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_KEYWORDS.no_pii.table.owner";
grant all privileges on table "MARKETING"."GOOGLE_SEARCH_ADS_360"."REPORT_KEYWORDS" to role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_KEYWORDS.no_pii.table.owner";

grant select on table "MARKETING"."GOOGLE_SEARCH_ADS_360"."REPORT_KEYWORDS" to role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_KEYWORDS.no_pii.table.reader";
grant usage on database "MARKETING" to role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_KEYWORDS.no_pii.table.reader";
grant usage on schema "GOOGLE_SEARCH_ADS_360" to role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_KEYWORDS.no_pii.table.reader";

grant insert,update on table "MARKETING"."GOOGLE_SEARCH_ADS_360"."REPORT_KEYWORDS" to role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_KEYWORDS.no_pii.table.writer";
grant usage on database "MARKETING" to role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_KEYWORDS.no_pii.table.writer";
grant usage on schema "GOOGLE_SEARCH_ADS_360" to role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_KEYWORDS.no_pii.table.writer";

grant role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_KEYWORDS.no_pii.table.owner" to role "MARKETING.GOOGLE_SEARCH_ADS_360.no_pii.owner.role";





--owner role
grant all on schema GOOGLE_SEARCH_ADS_360 to role "MARKETING.GOOGLE_SEARCH_ADS_360.no_pii.owner.role";
grant usage on database "MARKETING" to role "MARKETING.GOOGLE_SEARCH_ADS_360.no_pii.owner.role";
grant usage on schema "GOOGLE_SEARCH_ADS_360" to role "MARKETING.GOOGLE_SEARCH_ADS_360.no_pii.owner.role";
grant role "MARKETING.GOOGLE_SEARCH_ADS_360.no_pii.owner.role" to role accountadmin;

--service role
grant create table, create view, create stage on schema "MARKETING"."GOOGLE_SEARCH_ADS_360" to role "MARKETING.GOOGLE_SEARCH_ADS_360.no_pii.service.role";
grant role "MARKETING.GOOGLE_SEARCH_ADS_360.no_pii.writer.role" to role "MARKETING.GOOGLE_SEARCH_ADS_360.no_pii.service.role";
grant role "MARKETING.GOOGLE_SEARCH_ADS_360.no_pii.reader.role" to role "MARKETING.GOOGLE_SEARCH_ADS_360.no_pii.service.role";
grant role "MARKETING.GOOGLE_SEARCH_ADS_360.no_pii.service.role" to role AIRFLOW_SERVICE_ROLE;

-- Reader
grant role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_CONVERSION_EVENTS.no_pii.table.reader" to role "MARKETING.GOOGLE_SEARCH_ADS_360.no_pii.reader.role";
grant role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_ADS.no_pii.table.reader" to role "MARKETING.GOOGLE_SEARCH_ADS_360.no_pii.reader.role";
grant role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_KEYWORDS.no_pii.table.reader" to role "MARKETING.GOOGLE_SEARCH_ADS_360.no_pii.reader.role";

--Writer
grant role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_CONVERSION_EVENTS.no_pii.table.writer" to role "MARKETING.GOOGLE_SEARCH_ADS_360.no_pii.writer.role";
grant role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_ADS.no_pii.table.writer" to role "MARKETING.GOOGLE_SEARCH_ADS_360.no_pii.writer.role";
grant role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_KEYWORDS.no_pii.table.writer" to role "MARKETING.GOOGLE_SEARCH_ADS_360.no_pii.writer.role";


--DBT No PII access
grant role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_CONVERSION_EVENTS.no_pii.table.reader" to role "TRANSFORMER";
grant role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_ADS.no_pii.table.reader" to role "TRANSFORMER";
grant role "MARKETING.GOOGLE_SEARCH_ADS_360.REPORT_KEYWORDS_BTS_COUNTRY.no_pii.table.reader" to role "TRANSFORMER";
