use role ACCOUNTADMIN;
create database HRIS_DATA;
use database HRIS_DATA;
create schema ULTIPRO;

--schema level roles (new roles)
create role "HRIS_DATA.ULTIPRO.pii.reader.role";
create role "HRIS_DATA.ULTIPRO.pii.writer.role";
create role "HRIS_DATA.ULTIPRO.pii.owner.role";
create role "HRIS_DATA.ULTIPRO.pii.service.role";


--table level roles

--achievers "RAAS_ACHIEVERS_AUDIT_PII"
create role "HRIS_DATA.ULTIPRO.RAAS_ACHIEVERS_AUDIT_PII.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.RAAS_ACHIEVERS_AUDIT_PII.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.RAAS_ACHIEVERS_AUDIT_PII.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."RAAS_ACHIEVERS_AUDIT_PII" to role "HRIS_DATA.ULTIPRO.RAAS_ACHIEVERS_AUDIT_PII.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."RAAS_ACHIEVERS_AUDIT_PII" to role "HRIS_DATA.ULTIPRO.RAAS_ACHIEVERS_AUDIT_PII.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."RAAS_ACHIEVERS_AUDIT_PII" to role "HRIS_DATA.ULTIPRO.RAAS_ACHIEVERS_AUDIT_PII.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.RAAS_ACHIEVERS_AUDIT_PII.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.RAAS_ACHIEVERS_AUDIT_PII.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."RAAS_ACHIEVERS_AUDIT_PII" to role "HRIS_DATA.ULTIPRO.RAAS_ACHIEVERS_AUDIT_PII.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.RAAS_ACHIEVERS_AUDIT_PII.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.RAAS_ACHIEVERS_AUDIT_PII.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.RAAS_ACHIEVERS_AUDIT_PII.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--RESTAPI_COMPENSATION_DETAILS_PII data
create role "HRIS_DATA.ULTIPRO.RESTAPI_COMPENSATION_DETAILS_PII.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.RESTAPI_COMPENSATION_DETAILS_PII.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.RESTAPI_COMPENSATION_DETAILS_PII.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."RESTAPI_COMPENSATION_DETAILS_PII" to role "HRIS_DATA.ULTIPRO.RESTAPI_COMPENSATION_DETAILS_PII.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."RESTAPI_COMPENSATION_DETAILS_PII" to role "HRIS_DATA.ULTIPRO.RESTAPI_COMPENSATION_DETAILS_PII.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."RESTAPI_COMPENSATION_DETAILS_PII" to role "HRIS_DATA.ULTIPRO.RESTAPI_COMPENSATION_DETAILS_PII.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.RESTAPI_COMPENSATION_DETAILS_PII.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.RESTAPI_COMPENSATION_DETAILS_PII.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."RESTAPI_COMPENSATION_DETAILS_PII" to role "HRIS_DATA.ULTIPRO.RESTAPI_COMPENSATION_DETAILS_PII.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.RESTAPI_COMPENSATION_DETAILS_PII.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.RESTAPI_COMPENSATION_DETAILS_PII.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.RESTAPI_COMPENSATION_DETAILS_PII.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--"RESTAPI_EMPLOYEE_CHANGES_PII"data
create role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_CHANGES_PII.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_CHANGES_PII.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_CHANGES_PII.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."RESTAPI_EMPLOYEE_CHANGES_PII" to role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_CHANGES_PII.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."RESTAPI_EMPLOYEE_CHANGES_PII" to role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_CHANGES_PII.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."RESTAPI_EMPLOYEE_CHANGES_PII" to role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_CHANGES_PII.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_CHANGES_PII.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_CHANGES_PII.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."RESTAPI_EMPLOYEE_CHANGES_PII" to role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_CHANGES_PII.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_CHANGES_PII.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_CHANGES_PII.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_CHANGES_PII.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--RESTAPI_EMPLOYMENT_DETAILS_PII data
create role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYMENT_DETAILS_PII.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYMENT_DETAILS_PII.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYMENT_DETAILS_PII.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."RESTAPI_EMPLOYMENT_DETAILS_PII" to role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYMENT_DETAILS_PII.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."RESTAPI_EMPLOYMENT_DETAILS_PII" to role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYMENT_DETAILS_PII.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."RESTAPI_EMPLOYMENT_DETAILS_PII" to role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYMENT_DETAILS_PII.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYMENT_DETAILS_PII.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYMENT_DETAILS_PII.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."RESTAPI_EMPLOYMENT_DETAILS_PII" to role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYMENT_DETAILS_PII.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYMENT_DETAILS_PII.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYMENT_DETAILS_PII.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYMENT_DETAILS_PII.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--RESTAPI_EMPLOYEE_JOB_HISTORY_DETAILS_PII data
create role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_JOB_HISTORY_DETAILS_PII.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_JOB_HISTORY_DETAILS_PII.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_JOB_HISTORY_DETAILS_PII.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."RESTAPI_EMPLOYEE_JOB_HISTORY_DETAILS_PII" to role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_JOB_HISTORY_DETAILS_PII.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."RESTAPI_EMPLOYEE_JOB_HISTORY_DETAILS_PII" to role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_JOB_HISTORY_DETAILS_PII.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."RESTAPI_EMPLOYEE_JOB_HISTORY_DETAILS_PII" to role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_JOB_HISTORY_DETAILS_PII.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_JOB_HISTORY_DETAILS_PII.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_JOB_HISTORY_DETAILS_PII.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."RESTAPI_EMPLOYEE_JOB_HISTORY_DETAILS_PII" to role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_JOB_HISTORY_DETAILS_PII.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_JOB_HISTORY_DETAILS_PII.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_JOB_HISTORY_DETAILS_PII.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_JOB_HISTORY_DETAILS_PII.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--RESTAPI_PERSON_DETAILS_PII data
create role "HRIS_DATA.ULTIPRO.RESTAPI_PERSON_DETAILS_PII.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.RESTAPI_PERSON_DETAILS_PII.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.RESTAPI_PERSON_DETAILS_PII.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."RESTAPI_PERSON_DETAILS_PII" to role "HRIS_DATA.ULTIPRO.RESTAPI_PERSON_DETAILS_PII.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."RESTAPI_PERSON_DETAILS_PII" to role "HRIS_DATA.ULTIPRO.RESTAPI_PERSON_DETAILS_PII.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."RESTAPI_PERSON_DETAILS_PII" to role "HRIS_DATA.ULTIPRO.RESTAPI_PERSON_DETAILS_PII.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.RESTAPI_PERSON_DETAILS_PII.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.RESTAPI_PERSON_DETAILS_PII.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."RESTAPI_PERSON_DETAILS_PII" to role "HRIS_DATA.ULTIPRO.RESTAPI_PERSON_DETAILS_PII.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.RESTAPI_PERSON_DETAILS_PII.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.RESTAPI_PERSON_DETAILS_PII.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.RESTAPI_PERSON_DETAILS_PII.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--RESTAPI_PTO_PLANS_PII data
create role "HRIS_DATA.ULTIPRO.RESTAPI_PTO_PLANS_PII.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.RESTAPI_PTO_PLANS_PII.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.RESTAPI_PTO_PLANS_PII.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."RESTAPI_PTO_PLANS_PII" to role "HRIS_DATA.ULTIPRO.RESTAPI_PTO_PLANS_PII.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."RESTAPI_PTO_PLANS_PII" to role "HRIS_DATA.ULTIPRO.RESTAPI_PTO_PLANS_PII.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."RESTAPI_PTO_PLANS_PII" to role "HRIS_DATA.ULTIPRO.RESTAPI_PTO_PLANS_PII.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.RESTAPI_PTO_PLANS_PII.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.RESTAPI_PTO_PLANS_PII.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."RESTAPI_PTO_PLANS_PII" to role "HRIS_DATA.ULTIPRO.RESTAPI_PTO_PLANS_PII.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.RESTAPI_PTO_PLANS_PII.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.RESTAPI_PTO_PLANS_PII.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.RESTAPI_PTO_PLANS_PII.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--SERVICE_EMPLOYEE_COMPENSATION_PII data
create role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_COMPENSATION_PII.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_COMPENSATION_PII.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_COMPENSATION_PII.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."SERVICE_EMPLOYEE_COMPENSATION_PII" to role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_COMPENSATION_PII.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."SERVICE_EMPLOYEE_COMPENSATION_PII" to role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_COMPENSATION_PII.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."SERVICE_EMPLOYEE_COMPENSATION_PII" to role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_COMPENSATION_PII.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_COMPENSATION_PII.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_COMPENSATION_PII.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."SERVICE_EMPLOYEE_COMPENSATION_PII" to role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_COMPENSATION_PII.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_COMPENSATION_PII.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_COMPENSATION_PII.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_COMPENSATION_PII.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--SERVICE_EMPLOYEE_EMPLOYMENT_INFORMATION_PII data
create role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_EMPLOYMENT_INFORMATION_PII.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_EMPLOYMENT_INFORMATION_PII.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_EMPLOYMENT_INFORMATION_PII.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."SERVICE_EMPLOYEE_EMPLOYMENT_INFORMATION_PII" to role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_EMPLOYMENT_INFORMATION_PII.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."SERVICE_EMPLOYEE_EMPLOYMENT_INFORMATION_PII" to role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_EMPLOYMENT_INFORMATION_PII.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."SERVICE_EMPLOYEE_EMPLOYMENT_INFORMATION_PII" to role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_EMPLOYMENT_INFORMATION_PII.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_EMPLOYMENT_INFORMATION_PII.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_EMPLOYMENT_INFORMATION_PII.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."SERVICE_EMPLOYEE_EMPLOYMENT_INFORMATION_PII" to role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_EMPLOYMENT_INFORMATION_PII.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_EMPLOYMENT_INFORMATION_PII.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_EMPLOYMENT_INFORMATION_PII.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_EMPLOYMENT_INFORMATION_PII.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--SERVICE_EMPLOYEE_PAY_STATEMENT_PII data
create role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_PAY_STATEMENT_PII.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_PAY_STATEMENT_PII.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_PAY_STATEMENT_PII.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."SERVICE_EMPLOYEE_PAY_STATEMENT_PII" to role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_PAY_STATEMENT_PII.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."SERVICE_EMPLOYEE_PAY_STATEMENT_PII" to role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_PAY_STATEMENT_PII.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."SERVICE_EMPLOYEE_PAY_STATEMENT_PII" to role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_PAY_STATEMENT_PII.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_PAY_STATEMENT_PII.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_PAY_STATEMENT_PII.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."SERVICE_EMPLOYEE_PAY_STATEMENT_PII" to role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_PAY_STATEMENT_PII.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_PAY_STATEMENT_PII.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_PAY_STATEMENT_PII.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_PAY_STATEMENT_PII.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--TIME_MANAGEMENT_ACCESS_GROUPS data
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ACCESS_GROUPS.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ACCESS_GROUPS.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ACCESS_GROUPS.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_ACCESS_GROUPS" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ACCESS_GROUPS.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_ACCESS_GROUPS" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ACCESS_GROUPS.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_ACCESS_GROUPS" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ACCESS_GROUPS.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ACCESS_GROUPS.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ACCESS_GROUPS.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_ACCESS_GROUPS" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ACCESS_GROUPS.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ACCESS_GROUPS.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ACCESS_GROUPS.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ACCESS_GROUPS.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--TIME_MANAGEMENT_EMPLOYEE_PII data
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_EMPLOYEE_PII.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_EMPLOYEE_PII.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_EMPLOYEE_PII.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_EMPLOYEE_PII" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_EMPLOYEE_PII.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_EMPLOYEE_PII" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_EMPLOYEE_PII.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_EMPLOYEE_PII" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_EMPLOYEE_PII.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_EMPLOYEE_PII.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_EMPLOYEE_PII.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_EMPLOYEE_PII" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_EMPLOYEE_PII.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_EMPLOYEE_PII.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_EMPLOYEE_PII.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_EMPLOYEE_PII.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--TIME_MANAGEMENT_JOB data
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_JOB.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_JOB.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_JOB.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_JOB" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_JOB.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_JOB" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_JOB.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_JOB" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_JOB.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_JOB.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_JOB.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_JOB" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_JOB.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_JOB.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_JOB.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_JOB.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--TIME_MANAGEMENT_LOCATION data
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_LOCATION.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_LOCATION.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_LOCATION.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_LOCATION" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_LOCATION.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_LOCATION" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_LOCATION.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_LOCATION" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_LOCATION.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_LOCATION.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_LOCATION.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_LOCATION" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_LOCATION.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_LOCATION.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_LOCATION.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_LOCATION.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--TIME_MANAGEMENT_ORG_LEVEL_1 data
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_1.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_1.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_1.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_ORG_LEVEL_1" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_1.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_ORG_LEVEL_1" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_1.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_ORG_LEVEL_1" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_1.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_1.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_1.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_ORG_LEVEL_1" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_1.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_1.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_1.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_1.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--TIME_MANAGEMENT_ORG_LEVEL_2 data
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_2.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_2.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_2.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_ORG_LEVEL_2" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_2.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_ORG_LEVEL_2" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_2.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_ORG_LEVEL_2" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_2.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_2.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_2.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_ORG_LEVEL_2" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_2.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_2.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_2.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_2.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--TIME_MANAGEMENT_ORG_LEVEL_3 data
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_3.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_3.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_3.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_ORG_LEVEL_3" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_3.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_ORG_LEVEL_3" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_3.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_ORG_LEVEL_3" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_3.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_3.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_3.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_ORG_LEVEL_3" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_3.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_3.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_3.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_3.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--TIME_MANAGEMENT_ORG_LEVEL_4 data
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_4.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_4.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_4.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_ORG_LEVEL_4" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_4.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_ORG_LEVEL_4" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_4.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_ORG_LEVEL_4" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_4.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_4.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_4.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_ORG_LEVEL_4" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_4.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_4.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_4.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_4.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--TIME_MANAGEMENT_PAYCODE data
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYCODE.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYCODE.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYCODE.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_PAYCODE" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYCODE.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_PAYCODE" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYCODE.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_PAYCODE" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYCODE.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYCODE.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYCODE.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_PAYCODE" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYCODE.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYCODE.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYCODE.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYCODE.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--TIME_MANAGEMENT_PAYGROUP data
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYGROUP.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYGROUP.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYGROUP.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_PAYGROUP" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYGROUP.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_PAYGROUP" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYGROUP.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_PAYGROUP" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYGROUP.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYGROUP.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYGROUP.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_PAYGROUP" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYGROUP.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYGROUP.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYGROUP.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYGROUP.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--TIME_MANAGEMENT_PROJECT data
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PROJECT.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PROJECT.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PROJECT.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_PROJECT" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PROJECT.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_PROJECT" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PROJECT.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_PROJECT" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PROJECT.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PROJECT.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PROJECT.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_PROJECT" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PROJECT.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PROJECT.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PROJECT.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PROJECT.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--TIME_MANAGEMENT_REASON data
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_REASON.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_REASON.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_REASON.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_REASON" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_REASON.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_REASON" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_REASON.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_REASON" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_REASON.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_REASON.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_REASON.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_REASON" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_REASON.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_REASON.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_REASON.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_REASON.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--TIME_MANAGEMENT_SCHEDULE data
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_SCHEDULE" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_SCHEDULE" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_SCHEDULE" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_SCHEDULE" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--TIME_MANAGEMENT_SCHEDULE_REQUEST data
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE_REQUEST.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE_REQUEST.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE_REQUEST.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_SCHEDULE_REQUEST" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE_REQUEST.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_SCHEDULE_REQUEST" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE_REQUEST.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_SCHEDULE_REQUEST" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE_REQUEST.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE_REQUEST.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE_REQUEST.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_SCHEDULE_REQUEST" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE_REQUEST.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE_REQUEST.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE_REQUEST.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE_REQUEST.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--TIME_MANAGEMENT_SHIFT data
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFT.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFT.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFT.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_SHIFT" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFT.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_SHIFT" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFT.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_SHIFT" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFT.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFT.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFT.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_SHIFT" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFT.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFT.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFT.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFT.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--TIME_MANAGEMENT_SHIFTDET data
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFTDET.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFTDET.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFTDET.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_SHIFTDET" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFTDET.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_SHIFTDET" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFTDET.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_SHIFTDET" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFTDET.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFTDET.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFTDET.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_SHIFTDET" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFTDET.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFTDET.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFTDET.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFTDET.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--TIME_MANAGEMENT_TIME_PII data
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIME_PII.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIME_PII.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIME_PII.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_TIME_PII" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIME_PII.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_TIME_PII" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIME_PII.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_TIME_PII" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIME_PII.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIME_PII.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIME_PII.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_TIME_PII" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIME_PII.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIME_PII.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIME_PII.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIME_PII.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--TIME_MANAGEMENT_TIMESHEET data
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIMESHEET.pii.table.owner";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIMESHEET.pii.table.reader";
create role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIMESHEET.pii.table.writer";

grant ownership on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_TIMESHEET" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIMESHEET.pii.table.owner";
grant all privileges on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_TIMESHEET" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIMESHEET.pii.table.owner";

grant select on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_TIMESHEET" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIMESHEET.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIMESHEET.pii.table.reader";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIMESHEET.pii.table.reader";

grant insert,update on table "HRIS_DATA"."ULTIPRO"."TIME_MANAGEMENT_TIMESHEET" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIMESHEET.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIMESHEET.pii.table.writer";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIMESHEET.pii.table.writer";

grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIMESHEET.pii.table.owner" to role "HRIS_DATA.ULTIPRO.pii.owner.role";

--owner role

grant all on schema ULTIPRO to role "HRIS_DATA.ULTIPRO.pii.owner.role";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.ULTIPRO.pii.owner.role";
grant usage on schema "ULTIPRO" to role "HRIS_DATA.ULTIPRO.pii.owner.role";
grant role "HRIS_DATA.ULTIPRO.pii.owner.role" to role accountadmin

--service role
grant create table, create view, create stage on schema "HRIS_DATA"."ULTIPRO" to role "HRIS_DATA.ULTIPRO.pii.service.role";
grant role "HRIS_DATA.ULTIPRO.pii.writer.role" to role "HRIS_DATA.ULTIPRO.pii.service.role";

grant role "HRIS_DATA.ULTIPRO.pii.service.role" to role AIRFLOW_SERVICE_ROLE;


-- Reader
grant role "HRIS_DATA.ULTIPRO.RAAS_ACHIEVERS_AUDIT_PII.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.RESTAPI_COMPENSATION_DETAILS_PII.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_CHANGES_PII.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYMENT_DETAILS_PII.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_JOB_HISTORY_DETAILS_PII.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.RESTAPI_PERSON_DETAILS_PII.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.RESTAPI_PTO_PLANS_PII.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_COMPENSATION_PII.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_EMPLOYMENT_INFORMATION_PII.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_PAY_STATEMENT_PII.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ACCESS_GROUPS.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_EMPLOYEE_PII.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_JOB.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_LOCATION.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_1.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_1.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_2.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_3.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_4.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYCODE.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYGROUP.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PROJECT.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_REASON.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE_REQUEST.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFT.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFTDET.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIME_PII.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIMESHEET.pii.table.reader" to role "HRIS_DATA.ULTIPRO.pii.reader.role";

--Writer
grant role "HRIS_DATA.ULTIPRO.RAAS_ACHIEVERS_AUDIT_PII.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.RESTAPI_COMPENSATION_DETAILS_PII.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_CHANGES_PII.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYMENT_DETAILS_PII.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.RESTAPI_EMPLOYEE_JOB_HISTORY_DETAILS_PII.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.RESTAPI_PERSON_DETAILS_PII.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.RESTAPI_PTO_PLANS_PII.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_COMPENSATION_PII.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_EMPLOYMENT_INFORMATION_PII.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.SERVICE_EMPLOYEE_PAY_STATEMENT_PII.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ACCESS_GROUPS.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_EMPLOYEE_PII.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_JOB.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_LOCATION.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_1.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_1.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_2.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_3.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_ORG_LEVEL_4.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYCODE.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PAYGROUP.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_PROJECT.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_REASON.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SCHEDULE_REQUEST.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFT.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_SHIFTDET.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIME_PII.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";
grant role "HRIS_DATA.ULTIPRO.TIME_MANAGEMENT_TIMESHEET.pii.table.writer" to role "HRIS_DATA.ULTIPRO.pii.writer.role";



--for analysts
grant usage on warehouse TRANSFORMING_WAREHOUSE to role "TRANSFORMING_WAREHOUSE.HRIS_DATA.ULTIPRO.pii.reader.role";
grant role "HRIS_DATA.ULTIPRO.pii.reader.role" to role "transforming_warehouse.analytics.finance.pii.role";
grant role "HRIS_DATA.ULTIPRO.pii.reader.role" to role "TRANSFORMING_WAREHOUSE.HRIS_DATA.ULTIPRO.pii.reader.role"