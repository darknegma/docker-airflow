
--schema level roles
create role "MEDICAL_DATA.TRUEVAULT.pii.reader.role";
create role "MEDICAL_DATA.TRUEVAULT.pii.writer.role";
create role "MEDICAL_DATA.TRUEVAULT.pii.owner.role";
create role "MEDICAL_DATA.TRUEVAULT.pii.service.role";


--table level roles

-- DENTAL_HEALTH_QUESTIONNAIRE
create role "MEDICAL_DATA.TRUEVAULT.DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.owner";
create role "MEDICAL_DATA.TRUEVAULT.DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.reader";
create role "MEDICAL_DATA.TRUEVAULT.DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.writer";

grant ownership on table "MEDICAL_DATA"."TRUEVAULT"."DENTAL_HEALTH_QUESTIONNAIRE_PII" to role "MEDICAL_DATA.TRUEVAULT.DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.owner";
grant all privileges on table "MEDICAL_DATA"."TRUEVAULT"."DENTAL_HEALTH_QUESTIONNAIRE_PII" to role "MEDICAL_DATA.TRUEVAULT.DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.owner";

grant select on table "MEDICAL_DATA"."TRUEVAULT"."DENTAL_HEALTH_QUESTIONNAIRE_PII" to role "MEDICAL_DATA.TRUEVAULT.DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.reader";
grant usage on database "MEDICAL_DATA" to role "MEDICAL_DATA.TRUEVAULT.DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.reader";
grant usage on schema "TRUEVAULT" to role "MEDICAL_DATA.TRUEVAULT.DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.reader";

grant insert,update on table "MEDICAL_DATA"."TRUEVAULT"."DENTAL_HEALTH_QUESTIONNAIRE_PII" to role "MEDICAL_DATA.TRUEVAULT.DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.writer";
grant usage on database "MEDICAL_DATA" to role "MEDICAL_DATA.TRUEVAULT.DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.writer";
grant usage on schema "TRUEVAULT" to role "MEDICAL_DATA.TRUEVAULT.DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.writer";

grant role "MEDICAL_DATA.TRUEVAULT.DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.owner" to role "MEDICAL_DATA.TRUEVAULT.pii.owner.role";

-- FACT_DENTAL_HEALTH_QUESTIONNAIRE_PII
create role "MEDICAL_DATA.TRUEVAULT.FACT_DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.owner";
create role "MEDICAL_DATA.TRUEVAULT.FACT_DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.reader";
create role "MEDICAL_DATA.TRUEVAULT.FACT_DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.writer";

grant ownership on table "MEDICAL_DATA"."TRUEVAULT"."FACT_DENTAL_HEALTH_QUESTIONNAIRE_PII" to role "MEDICAL_DATA.TRUEVAULT.FACT_DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.owner";
grant all privileges on table "MEDICAL_DATA"."TRUEVAULT"."FACT_DENTAL_HEALTH_QUESTIONNAIRE_PII" to role "MEDICAL_DATA.TRUEVAULT.FACT_DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.owner";

grant select on table "MEDICAL_DATA"."TRUEVAULT"."FACT_DENTAL_HEALTH_QUESTIONNAIRE_PII" to role "MEDICAL_DATA.TRUEVAULT.FACT_DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.reader";
grant usage on database "MEDICAL_DATA" to role "MEDICAL_DATA.TRUEVAULT.FACT_DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.reader";
grant usage on schema "TRUEVAULT" to role "MEDICAL_DATA.TRUEVAULT.FACT_DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.reader";

grant insert,update on table "MEDICAL_DATA"."TRUEVAULT"."FACT_DENTAL_HEALTH_QUESTIONNAIRE_PII" to role "MEDICAL_DATA.TRUEVAULT.FACT_DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.writer";
grant usage on database "MEDICAL_DATA" to role "MEDICAL_DATA.TRUEVAULT.FACT_DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.writer";
grant usage on schema "TRUEVAULT" to role "MEDICAL_DATA.TRUEVAULT.FACT_DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.writer";

grant role "MEDICAL_DATA.TRUEVAULT.FACT_DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.owner" to role "MEDICAL_DATA.TRUEVAULT.pii.owner.role";


--owner role
grant all on schema "TRUEVAULT" to role "MEDICAL_DATA.TRUEVAULT.pii.owner.role";
grant usage on database "MEDICAL_DATA" to role "MEDICAL_DATA.TRUEVAULT.pii.owner.role";
grant usage on schema "TRUEVAULT" to role "MEDICAL_DATA.TRUEVAULT.pii.owner.role";
grant role "MEDICAL_DATA.TRUEVAULT.pii.owner.role" to role accountadmin;

--service role
grant create table, create view, create stage on schema "MEDICAL_DATA"."TRUEVAULT" to role "MEDICAL_DATA.TRUEVAULT.pii.service.role";
grant role "MEDICAL_DATA.TRUEVAULT.pii.writer.role" to role "MEDICAL_DATA.TRUEVAULT.pii.service.role";
grant role "MEDICAL_DATA.TRUEVAULT.pii.reader.role" to role "MEDICAL_DATA.TRUEVAULT.pii.service.role";
grant role "MEDICAL_DATA.TRUEVAULT.pii.service.role" to role AIRFLOW_SERVICE_ROLE;

-- Reader
grant role "MEDICAL_DATA.TRUEVAULT.DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.reader" to role "MEDICAL_DATA.TRUEVAULT.pii.reader.role";
grant role "MEDICAL_DATA.TRUEVAULT.FACT_DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.reader" to role "MEDICAL_DATA.TRUEVAULT.pii.reader.role";

--Writer
grant role "MEDICAL_DATA.TRUEVAULT.DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.writer" to role "MEDICAL_DATA.TRUEVAULT.pii.writer.role";
grant role "MEDICAL_DATA.TRUEVAULT.FACT_DENTAL_HEALTH_QUESTIONNAIRE_PII.pii.table.writer" to role "MEDICAL_DATA.TRUEVAULT.pii.writer.role";


--views

-- VW_DENTAL_HEALTH_QUESTIONNAIRE_VIEW_1
grant usage on database "MEDICAL_DATA" to role BUSINESS_INTELLIGENCE;
grant usage on schema "TRUEVAULT" to role BUSINESS_INTELLIGENCE;
grant select on "MEDICAL_DATA"."TRUEVAULT"."VW_DENTAL_HEALTH_QUESTIONNAIRE_VIEW_1" to role BUSINESS_INTELLIGENCE;
