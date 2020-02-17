
use role ACCOUNTADMIN;
create database FINANCE;
use database FINANCE;

CREATE ROLE "FINANCE.HFD.no_pii.reader.role";
CREATE ROLE "FINANCE.HFD.no_pii.writer.role";
CREATE ROLE "FINANCE.HFD.no_pii.owner.role";
CREATE ROLE "FINANCE.HFD.no_pii.service.role";

grant role "FINANCE.HFD.no_pii.writer.role" to role "FINANCE.HFD.no_pii.service.role";
grant role "FINANCE.HFD.no_pii.service.role" to user AIRFLOW;
grant usage on warehouse AIRFLOW_WAREHOUSE to role "FINANCE.HFD.no_pii.owner.role";
grant usage on warehouse AIRFLOW_WAREHOUSE to role "FINANCE.HFD.no_pii.service.role";

grant usage on database "FINANCE" to role "FINANCE.HFD.no_pii.owner.role";
grant usage on database "FINANCE" to role "FINANCE.HFD.no_pii.reader.role";
grant usage on database "FINANCE" to role "FINANCE.HFD.no_pii.writer.role";
grant usage on database "FINANCE" to role "FINANCE.HFD.no_pii.service.role";

grant usage on schema "FINANCE"."HFD" to role "FINANCE.HFD.no_pii.owner.role";
grant create table, create view, create stage on schema "FINANCE"."HFD" to role "FINANCE.HFD.no_pii.owner.role";
grant usage on schema "FINANCE"."HFD" to role "FINANCE.HFD.no_pii.service.role";
grant create table, create view, create stage on schema "FINANCE"."HFD" to role "FINANCE.HFD.no_pii.service.role";

grant usage on schema "FINANCE"."HFD" to role "FINANCE.HFD.no_pii.writer.role";
grant insert, update on all tables in schema "FINANCE"."HFD" to role "FINANCE.HFD.no_pii.writer.role";

grant usage on schema "FINANCE"."HFD" to role "FINANCE.HFD.no_pii.reader.role";



CREATE ROLE "FINANCE.HFD.pii.reader.role";
CREATE ROLE "FINANCE.HFD.pii.writer.role";
CREATE ROLE "FINANCE.HFD.pii.owner.role";
CREATE ROLE "FINANCE.HFD.pii.service.role";

grant role "FINANCE.HFD.pii.writer.role" to role "FINANCE.HFD.pii.service.role";
grant role "FINANCE.HFD.pii.service.role" to user AIRFLOW;
grant usage on warehouse AIRFLOW_WAREHOUSE to role "FINANCE.HFD.pii.owner.role";
grant usage on warehouse AIRFLOW_WAREHOUSE to role "FINANCE.HFD.pii.service.role";

grant usage on database "FINANCE" to role "FINANCE.HFD.pii.owner.role";
grant usage on database "FINANCE" to role "FINANCE.HFD.pii.reader.role";
grant usage on database "FINANCE" to role "FINANCE.HFD.pii.writer.role";
grant usage on database "FINANCE" to role "FINANCE.HFD.pii.service.role";

grant usage on schema "FINANCE"."HFD" to role "FINANCE.HFD.pii.owner.role";
grant create table, create view, create stage on schema "FINANCE"."HFD" to role "FINANCE.HFD.pii.owner.role";
grant usage on schema "FINANCE"."HFD" to role "FINANCE.HFD.pii.service.role";
grant create table, create view, create stage on schema "FINANCE"."HFD" to role "FINANCE.HFD.pii.service.role";

grant usage on schema "FINANCE"."HFD" to role "FINANCE.HFD.pii.writer.role";
grant insert, update on all tables in schema "FINANCE"."HFD" to role "FINANCE.HFD.pii.writer.role";

grant usage on schema "FINANCE"."HFD" to role "FINANCE.HFD.pii.reader.role";









