use role ACCOUNTADMIN;
create database MARKETING;
use database MARKETING;
create schema EXPERIAN;

--schema level roles
create role "MARKETING.EXPERIAN.no_pii.reader.role";
create role "MARKETING.EXPERIAN.no_pii.writer.role";
create role "MARKETING.EXPERIAN.no_pii.owner.role";
create role "MARKETING.EXPERIAN.no_pii.service.role";


grant usage on database MARKETING to role "MARKETING.EXPERIAN.no_pii.owner.role";
grant usage on database MARKETING to role "MARKETING.EXPERIAN.no_pii.writer.role";
grant usage on database MARKETING to role "MARKETING.EXPERIAN.no_pii.reader.role";
grant usage on database MARKETING to role "MARKETING.EXPERIAN.no_pii.service.role";


grant all on schema EXPERIAN to role "MARKETING.EXPERIAN.no_pii.owner.role";
grant usage on schema EXPERIAN to role "MARKETING.EXPERIAN.no_pii.writer.role";
grant usage on schema EXPERIAN to role "MARKETING.EXPERIAN.no_pii.reader.role";
grant usage on schema EXPERIAN to role "MARKETING.EXPERIAN.no_pii.service.role";


--block level roles
create role "MARKETING.EXPERIAN.BLOCK_LEVEL_DATA_NO_PII.no_pii.table.owner";
create role "MARKETING.EXPERIAN.BLOCK_LEVEL_DATA_NO_PII.no_pii.table.reader";
create role "MARKETING.EXPERIAN.BLOCK_LEVEL_DATA_NO_PII.no_pii.table.writer";


grant ownership on table "MARKETING"."EXPERIAN"."BLOCK_LEVEL_DATA_NO_PII" to role "MARKETING.EXPERIAN.BLOCK_LEVEL_DATA_NO_PII.no_pii.table.owner";
grant all privileges on table "MARKETING"."EXPERIAN"."BLOCK_LEVEL_DATA_NO_PII" to role "MARKETING.EXPERIAN.BLOCK_LEVEL_DATA_NO_PII.no_pii.table.owner";


grant select on table "MARKETING"."EXPERIAN"."BLOCK_LEVEL_DATA_NO_PII" to role "MARKETING.EXPERIAN.BLOCK_LEVEL_DATA_NO_PII.no_pii.table.reader";
grant usage on database "MARKETING" to role "MARKETING.EXPERIAN.BLOCK_LEVEL_DATA_NO_PII.no_pii.table.reader";
grant usage on schema "EXPERIAN" to role "MARKETING.EXPERIAN.BLOCK_LEVEL_DATA_NO_PII.no_pii.table.reader";


grant insert,update on table "MARKETING"."EXPERIAN"."BLOCK_LEVEL_DATA_NO_PII" to role "MARKETING.EXPERIAN.BLOCK_LEVEL_DATA_NO_PII.no_pii.table.writer";
grant usage on database "MARKETING" to role "MARKETING.EXPERIAN.BLOCK_LEVEL_DATA_NO_PII.no_pii.table.writer";
grant usage on schema "EXPERIAN" to role "MARKETING.EXPERIAN.BLOCK_LEVEL_DATA_NO_PII.no_pii.table.writer";


grant role "MARKETING.EXPERIAN.BLOCK_LEVEL_DATA_NO_PII.no_pii.table.owner" to role "MARKETING.EXPERIAN.no_pii.owner.role";



--Reader row
grant role "MARKETING.EXPERIAN.BLOCK_LEVEL_DATA_NO_PII.no_pii.table.reader" to role "MARKETING.EXPERIAN.no_pii.reader.role";


--Writer
grant usage, create table, create view, create stage on schema "MARKETING"."EXPERIAN" to role "MARKETING.EXPERIAN.no_pii.writer.role";



--service role

grant role "MARKETING.EXPERIAN.no_pii.writer.role" to role "MARKETING.EXPERIAN.no_pii.service.role";
grant role "MARKETING.EXPERIAN.no_pii.reader.role" to role "MARKETING.EXPERIAN.no_pii.service.role";

grant role "MARKETING.EXPERIAN.no_pii.service.role" to role "AIRFLOW_SERVICE_ROLE";



-----
--Pii version of experian goes here
------
create role "MARKETING.EXPERIAN.pii.reader.role";
create role "MARKETING.EXPERIAN.pii.writer.role";
create role "MARKETING.EXPERIAN.pii.owner.role";
create role "MARKETING.EXPERIAN.pii.service.role";

grant usage on database MARKETING to role "MARKETING.EXPERIAN.pii.owner.role";
grant usage on database MARKETING to role "MARKETING.EXPERIAN.pii.writer.role";
grant usage on database MARKETING to role "MARKETING.EXPERIAN.pii.reader.role";
grant usage on database MARKETING to role "MARKETING.EXPERIAN.pii.service.role";

grant all on schema EXPERIAN to role "MARKETING.EXPERIAN.pii.owner.role";
grant usage on schema EXPERIAN to role "MARKETING.EXPERIAN.pii.writer.role";
grant usage on schema EXPERIAN to role "MARKETING.EXPERIAN.pii.reader.role";
grant usage on schema EXPERIAN to role "MARKETING.EXPERIAN.pii.service.role";

create role "MARKETING.EXPERIAN.INDIV_HH_LEVEL_DATA_PII.pii.table.owner";
create role "MARKETING.EXPERIAN.INDIV_HH_LEVEL_DATA_PII.pii.table.reader";
create role "MARKETING.EXPERIAN.INDIV_HH_LEVEL_DATA_PII.pii.table.writer";

grant ownership on table "MARKETING"."EXPERIAN"."INDIV_HH_LEVEL_DATA_PII" to role "MARKETING.EXPERIAN.INDIV_HH_LEVEL_DATA_PII.pii.table.owner";
grant all privileges on table "MARKETING"."EXPERIAN"."INDIV_HH_LEVEL_DATA_PII" to role "MARKETING.EXPERIAN.INDIV_HH_LEVEL_DATA_PII.pii.table.owner";

grant select on table "MARKETING"."EXPERIAN"."INDIV_HH_LEVEL_DATA_PII" to role "MARKETING.EXPERIAN.INDIV_HH_LEVEL_DATA_PII.pii.table.reader";
grant usage on database "MARKETING" to role "MARKETING.EXPERIAN.INDIV_HH_LEVEL_DATA_PII.pii.table.reader";
grant usage on schema "EXPERIAN" to role "MARKETING.EXPERIAN.INDIV_HH_LEVEL_DATA_PII.pii.table.reader";

grant insert,update on table "MARKETING"."EXPERIAN"."INDIV_HH_LEVEL_DATA_PII" to role "MARKETING.EXPERIAN.INDIV_HH_LEVEL_DATA_PII.pii.table.writer";
grant usage on database "MARKETING" to role "MARKETING.EXPERIAN.INDIV_HH_LEVEL_DATA_PII.pii.table.writer";
grant usage on schema "EXPERIAN" to role "MARKETING.EXPERIAN.INDIV_HH_LEVEL_DATA_PII.pii.table.writer";

grant role "MARKETING.EXPERIAN.INDIV_HH_LEVEL_DATA_PII.pii.table.owner" to role "MARKETING.EXPERIAN.pii.owner.role";
grant role "MARKETING.EXPERIAN.INDIV_HH_LEVEL_DATA_PII.pii.table.reader" to role "MARKETING.EXPERIAN.pii.reader.role";
grant role "MARKETING.EXPERIAN.INDIV_HH_LEVEL_DATA_PII.pii.table.writer" to role "MARKETING.EXPERIAN.pii.writer.role";

grant usage, create table, create view, create stage on schema "MARKETING"."EXPERIAN" to role "MARKETING.EXPERIAN.pii.writer.role";

grant role "MARKETING.EXPERIAN.pii.writer.role" to role "MARKETING.EXPERIAN.pii.service.role";
grant role "MARKETING.EXPERIAN.pii.reader.role" to role "MARKETING.EXPERIAN.pii.service.role";

grant role "MARKETING.EXPERIAN.pii.service.role" to role "AIRFLOW_SERVICE_ROLE";

-- CREATE THE INDIVIDUAL TABLE ROLES
CREATE ROLE "MARKETING.EXPERIAN.INDIV_HH_LEVEL_DATA_PII.no_pii.view.reader";
GRANT USAGE ON DATABASE MARKETING TO ROLE "MARKETING.EXPERIAN.INDIV_HH_LEVEL_DATA_PII.no_pii.view.reader";
GRANT USAGE ON SCHEMA EXPERIAN TO ROLE "MARKETING.EXPERIAN.INDIV_HH_LEVEL_DATA_PII.no_pii.view.reader";

GRANT SELECT ON VIEW "MARKETING"."EXPERIAN"."VW_INDIV_HH_LEVEL_DATA_NO_PII" TO ROLE "MARKETING.EXPERIAN.INDIV_HH_LEVEL_DATA_PII.no_pii.view.reader";

GRANT ROLE "MARKETING.EXPERIAN.INDIV_HH_LEVEL_DATA_PII.no_pii.view.reader" TO ROLE "MARKETING.EXPERIAN.no_pii.reader.role";

-- REMOVE THE NO PII GROUP ROlE FROM THE PII ROLEs  ONCE TRANSFORMER CAN USE BOTH ROLES ANYWAYS
REVOKE  ROLE "MARKETING.EXPERIAN.no_pii.reader.role" FROM ROLE  "MARKETING.EXPERIAN.pii.reader.role";

-- ADD TO DBT ROLES
GRANT ROLE "MARKETING.EXPERIAN.no_pii.reader.role" TO ROLE DBT_DEV;
GRANT ROLE "MARKETING.EXPERIAN.no_pii.reader.role" TO ROLE TRANSFORMER;
GRANT ROLE "MARKETING.EXPERIAN.no_pii.reader.role" TO ROLE DBT_PROD;