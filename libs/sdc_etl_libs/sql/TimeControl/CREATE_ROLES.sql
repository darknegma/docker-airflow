use role ACCOUNTADMIN;
create database HRIS_DATA;
use database HRIS_DATA;
create schema TIMECONTROL;

--schema level roles (new roles)
create role "HRIS_DATA.TIMECONTROL.pii.reader.role";
create role "HRIS_DATA.TIMECONTROL.pii.writer.role";
create role "HRIS_DATA.TIMECONTROL.pii.owner.role";
create role "HRIS_DATA.TIMECONTROL.pii.service.role";


--table level roles

-- CHARGES
create role "HRIS_DATA.TIMECONTROL.CHARGES.pii.table.owner";
create role "HRIS_DATA.TIMECONTROL.CHARGES.pii.table.reader";
create role "HRIS_DATA.TIMECONTROL.CHARGES.pii.table.writer";

grant ownership on table "HRIS_DATA"."TIMECONTROL"."CHARGES" to role "HRIS_DATA.TIMECONTROL.CHARGES.pii.table.owner";
grant all privileges on table "HRIS_DATA"."TIMECONTROL"."CHARGES" to role "HRIS_DATA.TIMECONTROL.CHARGES.pii.table.owner";

grant select on table "HRIS_DATA"."TIMECONTROL"."CHARGES" to role "HRIS_DATA.TIMECONTROL.CHARGES.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.CHARGES.pii.table.reader";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.CHARGES.pii.table.reader";

grant insert,update on table "HRIS_DATA"."TIMECONTROL"."CHARGES" to role "HRIS_DATA.TIMECONTROL.CHARGES.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.CHARGES.pii.table.writer";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.CHARGES.pii.table.writer";

grant role "HRIS_DATA.TIMECONTROL.CHARGES.pii.table.owner" to role "HRIS_DATA.TIMECONTROL.pii.owner.role";

-- CHARGE_REVISIONS
create role "HRIS_DATA.TIMECONTROL.CHARGE_REVISIONS.pii.table.owner";
create role "HRIS_DATA.TIMECONTROL.CHARGE_REVISIONS.pii.table.reader";
create role "HRIS_DATA.TIMECONTROL.CHARGE_REVISIONS.pii.table.writer";

grant ownership on table "HRIS_DATA"."TIMECONTROL"."CHARGE_REVISIONS" to role "HRIS_DATA.TIMECONTROL.CHARGE_REVISIONS.pii.table.owner";
grant all privileges on table "HRIS_DATA"."TIMECONTROL"."CHARGE_REVISIONS" to role "HRIS_DATA.TIMECONTROL.CHARGE_REVISIONS.pii.table.owner";

grant select on table "HRIS_DATA"."TIMECONTROL"."CHARGE_REVISIONS" to role "HRIS_DATA.TIMECONTROL.CHARGE_REVISIONS.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.CHARGE_REVISIONS.pii.table.reader";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.CHARGE_REVISIONS.pii.table.reader";

grant insert,update on table "HRIS_DATA"."TIMECONTROL"."CHARGE_REVISIONS" to role "HRIS_DATA.TIMECONTROL.CHARGE_REVISIONS.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.CHARGE_REVISIONS.pii.table.writer";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.CHARGE_REVISIONS.pii.table.writer";

grant role "HRIS_DATA.TIMECONTROL.CHARGE_REVISIONS.pii.table.owner" to role "HRIS_DATA.TIMECONTROL.pii.owner.role";

-- EMPLOYEES_PII
create role "HRIS_DATA.TIMECONTROL.EMPLOYEES_PII.pii.table.owner";
create role "HRIS_DATA.TIMECONTROL.EMPLOYEES_PII.pii.table.reader";
create role "HRIS_DATA.TIMECONTROL.EMPLOYEES_PII.pii.table.writer";

grant ownership on table "HRIS_DATA"."TIMECONTROL"."EMPLOYEES_PII" to role "HRIS_DATA.TIMECONTROL.EMPLOYEES_PII.pii.table.owner";
grant all privileges on table "HRIS_DATA"."TIMECONTROL"."EMPLOYEES_PII" to role "HRIS_DATA.TIMECONTROL.EMPLOYEES_PII.pii.table.owner";

grant select on table "HRIS_DATA"."TIMECONTROL"."EMPLOYEES_PII" to role "HRIS_DATA.TIMECONTROL.EMPLOYEES_PII.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.EMPLOYEES_PII.pii.table.reader";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.EMPLOYEES_PII.pii.table.reader";

grant insert,update on table "HRIS_DATA"."TIMECONTROL"."EMPLOYEES_PII" to role "HRIS_DATA.TIMECONTROL.EMPLOYEES_PII.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.EMPLOYEES_PII.pii.table.writer";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.EMPLOYEES_PII.pii.table.writer";

grant role "HRIS_DATA.TIMECONTROL.EMPLOYEES_PII.pii.table.owner" to role "HRIS_DATA.TIMECONTROL.pii.owner.role";

-- EMPLOYEE_REVISIONS_PII
create role "HRIS_DATA.TIMECONTROL.EMPLOYEE_REVISIONS_PII.pii.table.owner";
create role "HRIS_DATA.TIMECONTROL.EMPLOYEE_REVISIONS_PII.pii.table.reader";
create role "HRIS_DATA.TIMECONTROL.EMPLOYEE_REVISIONS_PII.pii.table.writer";

grant ownership on table "HRIS_DATA"."TIMECONTROL"."EMPLOYEE_REVISIONS_PII" to role "HRIS_DATA.TIMECONTROL.EMPLOYEE_REVISIONS_PII.pii.table.owner";
grant all privileges on table "HRIS_DATA"."TIMECONTROL"."EMPLOYEE_REVISIONS_PII" to role "HRIS_DATA.TIMECONTROL.EMPLOYEE_REVISIONS_PII.pii.table.owner";

grant select on table "HRIS_DATA"."TIMECONTROL"."EMPLOYEE_REVISIONS_PII" to role "HRIS_DATA.TIMECONTROL.EMPLOYEE_REVISIONS_PII.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.EMPLOYEE_REVISIONS_PII.pii.table.reader";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.EMPLOYEE_REVISIONS_PII.pii.table.reader";

grant insert,update on table "HRIS_DATA"."TIMECONTROL"."EMPLOYEE_REVISIONS_PII" to role "HRIS_DATA.TIMECONTROL.EMPLOYEE_REVISIONS_PII.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.EMPLOYEE_REVISIONS_PII.pii.table.writer";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.EMPLOYEE_REVISIONS_PII.pii.table.writer";

grant role "HRIS_DATA.TIMECONTROL.EMPLOYEE_REVISIONS_PII.pii.table.owner" to role "HRIS_DATA.TIMECONTROL.pii.owner.role";

-- LANGUAGES
create role "HRIS_DATA.TIMECONTROL.LANGUAGES.pii.table.owner";
create role "HRIS_DATA.TIMECONTROL.LANGUAGES.pii.table.reader";
create role "HRIS_DATA.TIMECONTROL.LANGUAGES.pii.table.writer";

grant ownership on table "HRIS_DATA"."TIMECONTROL"."LANGUAGES" to role "HRIS_DATA.TIMECONTROL.LANGUAGES.pii.table.owner";
grant all privileges on table "HRIS_DATA"."TIMECONTROL"."LANGUAGES" to role "HRIS_DATA.TIMECONTROL.LANGUAGES.pii.table.owner";

grant select on table "HRIS_DATA"."TIMECONTROL"."LANGUAGES" to role "HRIS_DATA.TIMECONTROL.LANGUAGES.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.LANGUAGES.pii.table.reader";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.LANGUAGES.pii.table.reader";

grant insert,update on table "HRIS_DATA"."TIMECONTROL"."LANGUAGES" to role "HRIS_DATA.TIMECONTROL.LANGUAGES.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.LANGUAGES.pii.table.writer";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.LANGUAGES.pii.table.writer";

grant role "HRIS_DATA.TIMECONTROL.LANGUAGES.pii.table.owner" to role "HRIS_DATA.TIMECONTROL.pii.owner.role";

-- PERIODS_PAY
create role "HRIS_DATA.TIMECONTROL.PERIODS_PAY.pii.table.owner";
create role "HRIS_DATA.TIMECONTROL.PERIODS_PAY.pii.table.reader";
create role "HRIS_DATA.TIMECONTROL.PERIODS_PAY.pii.table.writer";

grant ownership on table "HRIS_DATA"."TIMECONTROL"."PERIODS_PAY" to role "HRIS_DATA.TIMECONTROL.PERIODS_PAY.pii.table.owner";
grant all privileges on table "HRIS_DATA"."TIMECONTROL"."PERIODS_PAY" to role "HRIS_DATA.TIMECONTROL.PERIODS_PAY.pii.table.owner";

grant select on table "HRIS_DATA"."TIMECONTROL"."PERIODS_PAY" to role "HRIS_DATA.TIMECONTROL.PERIODS_PAY.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.PERIODS_PAY.pii.table.reader";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.PERIODS_PAY.pii.table.reader";

grant insert,update on table "HRIS_DATA"."TIMECONTROL"."PERIODS_PAY" to role "HRIS_DATA.TIMECONTROL.PERIODS_PAY.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.PERIODS_PAY.pii.table.writer";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.PERIODS_PAY.pii.table.writer";

grant role "HRIS_DATA.TIMECONTROL.PERIODS_PAY.pii.table.owner" to role "HRIS_DATA.TIMECONTROL.pii.owner.role";

-- PERIODS_TIMESHEET
create role "HRIS_DATA.TIMECONTROL.PERIODS_TIMESHEET.pii.table.owner";
create role "HRIS_DATA.TIMECONTROL.PERIODS_TIMESHEET.pii.table.reader";
create role "HRIS_DATA.TIMECONTROL.PERIODS_TIMESHEET.pii.table.writer";

grant ownership on table "HRIS_DATA"."TIMECONTROL"."PERIODS_TIMESHEET" to role "HRIS_DATA.TIMECONTROL.PERIODS_TIMESHEET.pii.table.owner";
grant all privileges on table "HRIS_DATA"."TIMECONTROL"."PERIODS_TIMESHEET" to role "HRIS_DATA.TIMECONTROL.PERIODS_TIMESHEET.pii.table.owner";

grant select on table "HRIS_DATA"."TIMECONTROL"."PERIODS_TIMESHEET" to role "HRIS_DATA.TIMECONTROL.PERIODS_TIMESHEET.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.PERIODS_TIMESHEET.pii.table.reader";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.PERIODS_TIMESHEET.pii.table.reader";

grant insert,update on table "HRIS_DATA"."TIMECONTROL"."PERIODS_TIMESHEET" to role "HRIS_DATA.TIMECONTROL.PERIODS_TIMESHEET.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.PERIODS_TIMESHEET.pii.table.writer";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.PERIODS_TIMESHEET.pii.table.writer";

grant role "HRIS_DATA.TIMECONTROL.PERIODS_TIMESHEET.pii.table.owner" to role "HRIS_DATA.TIMECONTROL.pii.owner.role";

-- PROJECTS
create role "HRIS_DATA.TIMECONTROL.PROJECTS.pii.table.owner";
create role "HRIS_DATA.TIMECONTROL.PROJECTS.pii.table.reader";
create role "HRIS_DATA.TIMECONTROL.PROJECTS.pii.table.writer";

grant ownership on table "HRIS_DATA"."TIMECONTROL"."PROJECTS" to role "HRIS_DATA.TIMECONTROL.PROJECTS.pii.table.owner";
grant all privileges on table "HRIS_DATA"."TIMECONTROL"."PROJECTS" to role "HRIS_DATA.TIMECONTROL.PROJECTS.pii.table.owner";

grant select on table "HRIS_DATA"."TIMECONTROL"."PROJECTS" to role "HRIS_DATA.TIMECONTROL.PROJECTS.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.PROJECTS.pii.table.reader";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.PROJECTS.pii.table.reader";

grant insert,update on table "HRIS_DATA"."TIMECONTROL"."PROJECTS" to role "HRIS_DATA.TIMECONTROL.PROJECTS.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.PROJECTS.pii.table.writer";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.PROJECTS.pii.table.writer";

grant role "HRIS_DATA.TIMECONTROL.PROJECTS.pii.table.owner" to role "HRIS_DATA.TIMECONTROL.pii.owner.role";

-- REPORTS
create role "HRIS_DATA.TIMECONTROL.REPORTS.pii.table.owner";
create role "HRIS_DATA.TIMECONTROL.REPORTS.pii.table.reader";
create role "HRIS_DATA.TIMECONTROL.REPORTS.pii.table.writer";

grant ownership on table "HRIS_DATA"."TIMECONTROL"."REPORTS" to role "HRIS_DATA.TIMECONTROL.REPORTS.pii.table.owner";
grant all privileges on table "HRIS_DATA"."TIMECONTROL"."REPORTS" to role "HRIS_DATA.TIMECONTROL.REPORTS.pii.table.owner";

grant select on table "HRIS_DATA"."TIMECONTROL"."REPORTS" to role "HRIS_DATA.TIMECONTROL.REPORTS.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.REPORTS.pii.table.reader";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.REPORTS.pii.table.reader";

grant insert,update on table "HRIS_DATA"."TIMECONTROL"."REPORTS" to role "HRIS_DATA.TIMECONTROL.REPORTS.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.REPORTS.pii.table.writer";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.REPORTS.pii.table.writer";

grant role "HRIS_DATA.TIMECONTROL.REPORTS.pii.table.owner" to role "HRIS_DATA.TIMECONTROL.pii.owner.role";

-- RESOURCES
create role "HRIS_DATA.TIMECONTROL.RESOURCES.pii.table.owner";
create role "HRIS_DATA.TIMECONTROL.RESOURCES.pii.table.reader";
create role "HRIS_DATA.TIMECONTROL.RESOURCES.pii.table.writer";

grant ownership on table "HRIS_DATA"."TIMECONTROL"."RESOURCES" to role "HRIS_DATA.TIMECONTROL.RESOURCES.pii.table.owner";
grant all privileges on table "HRIS_DATA"."TIMECONTROL"."RESOURCES" to role "HRIS_DATA.TIMECONTROL.RESOURCES.pii.table.owner";

grant select on table "HRIS_DATA"."TIMECONTROL"."RESOURCES" to role "HRIS_DATA.TIMECONTROL.RESOURCES.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.RESOURCES.pii.table.reader";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.RESOURCES.pii.table.reader";

grant insert,update on table "HRIS_DATA"."TIMECONTROL"."RESOURCES" to role "HRIS_DATA.TIMECONTROL.RESOURCES.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.RESOURCES.pii.table.writer";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.RESOURCES.pii.table.writer";

grant role "HRIS_DATA.TIMECONTROL.RESOURCES.pii.table.owner" to role "HRIS_DATA.TIMECONTROL.pii.owner.role";

-- RATES_PII
create role "HRIS_DATA.TIMECONTROL.RATES_PII.pii.table.owner";
create role "HRIS_DATA.TIMECONTROL.RATES_PII.pii.table.reader";
create role "HRIS_DATA.TIMECONTROL.RATES_PII.pii.table.writer";

grant ownership on table "HRIS_DATA"."TIMECONTROL"."RATES_PII" to role "HRIS_DATA.TIMECONTROL.RATES_PII.pii.table.owner";
grant all privileges on table "HRIS_DATA"."TIMECONTROL"."RATES_PII" to role "HRIS_DATA.TIMECONTROL.RATES_PII.pii.table.owner";

grant select on table "HRIS_DATA"."TIMECONTROL"."RATES_PII" to role "HRIS_DATA.TIMECONTROL.RATES_PII.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.RATES_PII.pii.table.reader";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.RATES_PII.pii.table.reader";

grant insert,update on table "HRIS_DATA"."TIMECONTROL"."RATES_PII" to role "HRIS_DATA.TIMECONTROL.RATES_PII.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.RATES_PII.pii.table.writer";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.RATES_PII.pii.table.writer";

grant role "HRIS_DATA.TIMECONTROL.RATES_PII.pii.table.owner" to role "HRIS_DATA.TIMECONTROL.pii.owner.role";

-- TIMESHEETS_DETAILS_PII
create role "HRIS_DATA.TIMECONTROL.TIMESHEETS_DETAILS_PII.pii.table.owner";
create role "HRIS_DATA.TIMECONTROL.TIMESHEETS_DETAILS_PII.pii.table.reader";
create role "HRIS_DATA.TIMECONTROL.TIMESHEETS_DETAILS_PII.pii.table.writer";

grant ownership on table "HRIS_DATA"."TIMECONTROL"."TIMESHEETS_DETAILS_PII" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_DETAILS_PII.pii.table.owner";
grant all privileges on table "HRIS_DATA"."TIMECONTROL"."TIMESHEETS_DETAILS_PII" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_DETAILS_PII.pii.table.owner";

grant select on table "HRIS_DATA"."TIMECONTROL"."TIMESHEETS_DETAILS_PII" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_DETAILS_PII.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_DETAILS_PII.pii.table.reader";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_DETAILS_PII.pii.table.reader";

grant insert,update on table "HRIS_DATA"."TIMECONTROL"."TIMESHEETS_DETAILS_PII" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_DETAILS_PII.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_DETAILS_PII.pii.table.writer";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_DETAILS_PII.pii.table.writer";

grant role "HRIS_DATA.TIMECONTROL.TIMESHEETS_DETAILS_PII.pii.table.owner" to role "HRIS_DATA.TIMECONTROL.pii.owner.role";

-- TIMESHEETS_PII
create role "HRIS_DATA.TIMECONTROL.TIMESHEETS_PII.pii.table.owner";
create role "HRIS_DATA.TIMECONTROL.TIMESHEETS_PII.pii.table.reader";
create role "HRIS_DATA.TIMECONTROL.TIMESHEETS_PII.pii.table.writer";

grant ownership on table "HRIS_DATA"."TIMECONTROL"."TIMESHEETS_PII" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_PII.pii.table.owner";
grant all privileges on table "HRIS_DATA"."TIMECONTROL"."TIMESHEETS_PII" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_PII.pii.table.owner";

grant select on table "HRIS_DATA"."TIMECONTROL"."TIMESHEETS_PII" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_PII.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_PII.pii.table.reader";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_PII.pii.table.reader";

grant insert,update on table "HRIS_DATA"."TIMECONTROL"."TIMESHEETS_PII" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_PII.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_PII.pii.table.writer";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_PII.pii.table.writer";

grant role "HRIS_DATA.TIMECONTROL.TIMESHEETS_PII.pii.table.owner" to role "HRIS_DATA.TIMECONTROL.pii.owner.role";

-- TIMESHEETS_POSTED_DETAILS_PII
create role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_DETAILS_PII.pii.table.owner";
create role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_DETAILS_PII.pii.table.reader";
create role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_DETAILS_PII.pii.table.writer";

grant ownership on table "HRIS_DATA"."TIMECONTROL"."TIMESHEETS_POSTED_DETAILS_PII" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_DETAILS_PII.pii.table.owner";
grant all privileges on table "HRIS_DATA"."TIMECONTROL"."TIMESHEETS_POSTED_DETAILS_PII" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_DETAILS_PII.pii.table.owner";

grant select on table "HRIS_DATA"."TIMECONTROL"."TIMESHEETS_POSTED_DETAILS_PII" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_DETAILS_PII.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_DETAILS_PII.pii.table.reader";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_DETAILS_PII.pii.table.reader";

grant insert,update on table "HRIS_DATA"."TIMECONTROL"."TIMESHEETS_POSTED_DETAILS_PII" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_DETAILS_PII.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_DETAILS_PII.pii.table.writer";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_DETAILS_PII.pii.table.writer";

grant role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_DETAILS_PII.pii.table.owner" to role "HRIS_DATA.TIMECONTROL.pii.owner.role";

-- TIMESHEETS_POSTED_PII
create role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_PII.pii.table.owner";
create role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_PII.pii.table.reader";
create role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_PII.pii.table.writer";

grant ownership on table "HRIS_DATA"."TIMECONTROL"."TIMESHEETS_POSTED_PII" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_PII.pii.table.owner";
grant all privileges on table "HRIS_DATA"."TIMECONTROL"."TIMESHEETS_POSTED_PII" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_PII.pii.table.owner";

grant select on table "HRIS_DATA"."TIMECONTROL"."TIMESHEETS_POSTED_PII" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_PII.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_PII.pii.table.reader";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_PII.pii.table.reader";

grant insert,update on table "HRIS_DATA"."TIMECONTROL"."TIMESHEETS_POSTED_PII" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_PII.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_PII.pii.table.writer";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_PII.pii.table.writer";

grant role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_PII.pii.table.owner" to role "HRIS_DATA.TIMECONTROL.pii.owner.role";

-- USERS_PII
create role "HRIS_DATA.TIMECONTROL.USERS_PII.pii.table.owner";
create role "HRIS_DATA.TIMECONTROL.USERS_PII.pii.table.reader";
create role "HRIS_DATA.TIMECONTROL.USERS_PII.pii.table.writer";

grant ownership on table "HRIS_DATA"."TIMECONTROL"."USERS_PII" to role "HRIS_DATA.TIMECONTROL.USERS_PII.pii.table.owner";
grant all privileges on table "HRIS_DATA"."TIMECONTROL"."USERS_PII" to role "HRIS_DATA.TIMECONTROL.USERS_PII.pii.table.owner";

grant select on table "HRIS_DATA"."TIMECONTROL"."USERS_PII" to role "HRIS_DATA.TIMECONTROL.USERS_PII.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.USERS_PII.pii.table.reader";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.USERS_PII.pii.table.reader";

grant insert,update on table "HRIS_DATA"."TIMECONTROL"."USERS_PII" to role "HRIS_DATA.TIMECONTROL.USERS_PII.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.USERS_PII.pii.table.writer";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.USERS_PII.pii.table.writer";

grant role "HRIS_DATA.TIMECONTROL.USERS_PII.pii.table.owner" to role "HRIS_DATA.TIMECONTROL.pii.owner.role";

-- WBS
create role "HRIS_DATA.TIMECONTROL.WBS.pii.table.owner";
create role "HRIS_DATA.TIMECONTROL.WBS.pii.table.reader";
create role "HRIS_DATA.TIMECONTROL.WBS.pii.table.writer";

grant ownership on table "HRIS_DATA"."TIMECONTROL"."WBS" to role "HRIS_DATA.TIMECONTROL.WBS.pii.table.owner";
grant all privileges on table "HRIS_DATA"."TIMECONTROL"."WBS" to role "HRIS_DATA.TIMECONTROL.WBS.pii.table.owner";

grant select on table "HRIS_DATA"."TIMECONTROL"."WBS" to role "HRIS_DATA.TIMECONTROL.WBS.pii.table.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.WBS.pii.table.reader";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.WBS.pii.table.reader";

grant insert,update on table "HRIS_DATA"."TIMECONTROL"."WBS" to role "HRIS_DATA.TIMECONTROL.WBS.pii.table.writer";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.WBS.pii.table.writer";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.WBS.pii.table.writer";

grant role "HRIS_DATA.TIMECONTROL.WBS.pii.table.owner" to role "HRIS_DATA.TIMECONTROL.pii.owner.role";


--owner role
grant all on schema TIMECONTROL to role "HRIS_DATA.TIMECONTROL.pii.owner.role";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.pii.owner.role";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.pii.owner.role";
grant role "HRIS_DATA.TIMECONTROL.pii.owner.role" to role accountadmin

--service role
grant create table, create view, create stage on schema "HRIS_DATA"."TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.pii.service.role";
grant role "HRIS_DATA.TIMECONTROL.pii.writer.role" to role "HRIS_DATA.TIMECONTROL.pii.service.role";
grant role "HRIS_DATA.TIMECONTROL.pii.reader.role" to role "HRIS_DATA.TIMECONTROL.pii.service.role";
grant role "HRIS_DATA.TIMECONTROL.pii.service.role" to role AIRFLOW_SERVICE_ROLE;

-- Reader
grant role "HRIS_DATA.TIMECONTROL.CHARGES.pii.table.reader" to role "HRIS_DATA.TIMECONTROL.pii.reader.role";
grant role "HRIS_DATA.TIMECONTROL.CHARGE_REVISIONS.pii.table.reader" to role "HRIS_DATA.TIMECONTROL.pii.reader.role";
grant role "HRIS_DATA.TIMECONTROL.EMPLOYEES_PII.pii.table.reader" to role "HRIS_DATA.TIMECONTROL.pii.reader.role";
grant role "HRIS_DATA.TIMECONTROL.EMPLOYEE_REVISIONS_PII.pii.table.reader" to role "HRIS_DATA.TIMECONTROL.pii.reader.role";
grant role "HRIS_DATA.TIMECONTROL.LANGUAGES.pii.table.reader" to role "HRIS_DATA.TIMECONTROL.pii.reader.role";
grant role "HRIS_DATA.TIMECONTROL.PERIODS_PAY.pii.table.reader" to role "HRIS_DATA.TIMECONTROL.pii.reader.role";
grant role "HRIS_DATA.TIMECONTROL.PERIODS_TIMESHEET.pii.table.reader" to role "HRIS_DATA.TIMECONTROL.pii.reader.role";
grant role "HRIS_DATA.TIMECONTROL.PROJECTS.pii.table.reader" to role "HRIS_DATA.TIMECONTROL.pii.reader.role";
grant role "HRIS_DATA.TIMECONTROL.REPORTS.pii.table.reader" to role "HRIS_DATA.TIMECONTROL.pii.reader.role";
grant role "HRIS_DATA.TIMECONTROL.RESOURCES.pii.table.reader" to role "HRIS_DATA.TIMECONTROL.pii.reader.role";
grant role "HRIS_DATA.TIMECONTROL.TIMESHEETS_DETAILS_PII.pii.table.reader" to role "HRIS_DATA.TIMECONTROL.pii.reader.role";
grant role "HRIS_DATA.TIMECONTROL.TIMESHEETS_PII.pii.table.reader" to role "HRIS_DATA.TIMECONTROL.pii.reader.role";
grant role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_DETAILS_PII.pii.table.reader" to role "HRIS_DATA.TIMECONTROL.pii.reader.role";
grant role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_PII.pii.table.reader" to role "HRIS_DATA.TIMECONTROL.pii.reader.role";
grant role "HRIS_DATA.TIMECONTROL.USERS_PII.pii.table.reader" to role "HRIS_DATA.TIMECONTROL.pii.reader.role";
grant role "HRIS_DATA.TIMECONTROL.WBS.pii.table.reader" to role "HRIS_DATA.TIMECONTROL.pii.reader.role";
grant role "HRIS_DATA.TIMECONTROL.RATES_PII.pii.table.reader" to role "HRIS_DATA.TIMECONTROL.pii.reader.role";

--Writer
grant role "HRIS_DATA.TIMECONTROL.CHARGES.pii.table.writer" to role "HRIS_DATA.TIMECONTROL.pii.writer.role";
grant role "HRIS_DATA.TIMECONTROL.CHARGE_REVISIONS.pii.table.writer" to role "HRIS_DATA.TIMECONTROL.pii.writer.role";
grant role "HRIS_DATA.TIMECONTROL.EMPLOYEES_PII.pii.table.writer" to role "HRIS_DATA.TIMECONTROL.pii.writer.role";
grant role "HRIS_DATA.TIMECONTROL.EMPLOYEE_REVISIONS_PII.pii.table.writer" to role "HRIS_DATA.TIMECONTROL.pii.writer.role";
grant role "HRIS_DATA.TIMECONTROL.LANGUAGES.pii.table.writer" to role "HRIS_DATA.TIMECONTROL.pii.writer.role";
grant role "HRIS_DATA.TIMECONTROL.PERIODS_PAY.pii.table.writer" to role "HRIS_DATA.TIMECONTROL.pii.writer.role";
grant role "HRIS_DATA.TIMECONTROL.PERIODS_TIMESHEET.pii.table.writer" to role "HRIS_DATA.TIMECONTROL.pii.writer.role";
grant role "HRIS_DATA.TIMECONTROL.PROJECTS.pii.table.writer" to role "HRIS_DATA.TIMECONTROL.pii.writer.role";
grant role "HRIS_DATA.TIMECONTROL.REPORTS.pii.table.writer" to role "HRIS_DATA.TIMECONTROL.pii.writer.role";
grant role "HRIS_DATA.TIMECONTROL.RESOURCES.pii.table.writer" to role "HRIS_DATA.TIMECONTROL.pii.writer.role";
grant role "HRIS_DATA.TIMECONTROL.TIMESHEETS_DETAILS_PII.pii.table.writer" to role "HRIS_DATA.TIMECONTROL.pii.writer.role";
grant role "HRIS_DATA.TIMECONTROL.TIMESHEETS_PII.pii.table.writer" to role "HRIS_DATA.TIMECONTROL.pii.writer.role";
grant role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_DETAILS_PII.pii.table.writer" to role "HRIS_DATA.TIMECONTROL.pii.writer.role";
grant role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_PII.pii.table.writer" to role "HRIS_DATA.TIMECONTROL.pii.writer.role";
grant role "HRIS_DATA.TIMECONTROL.USERS_PII.pii.table.writer" to role "HRIS_DATA.TIMECONTROL.pii.writer.role";
grant role "HRIS_DATA.TIMECONTROL.WBS.pii.table.writer" to role "HRIS_DATA.TIMECONTROL.pii.writer.role";
grant role "HRIS_DATA.TIMECONTROL.RATES_PII.pii.table.writer" to role "HRIS_DATA.TIMECONTROL.pii.writer.role";




