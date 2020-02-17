-- UPDATE PLAN
-- 1 TABLES ARE BEING USED BY DBT
-- 1 NO_PII TABLES - CREATE THE INDIVIDUAL READ ROLES FOR THESE
-- 0 PII TABLE-- 0 VIEWS NEEDED
-- ADD THE  read ROLES TO THE NO_PII GROUP ROLE
-- ADD THE NO_PII GROUP ROLE TO BOTH DBT ROLES

-- RAW.REPORTING.DRDOWLING_OTHER_PROVIDERS

USE ROLE AIRFLOW_SERVICE_ROLE;
USE DATABASE RAW;
USE SCHEMA REPORTING;

USE ROLE SECURITYADMIN;

-- CREATE THE GROUP ROLES
CREATE ROLE "RAW.REPORTING.no_pii.reader.role" ;

-- ADD PERMISIONS
GRANT USAGE ON DATABASE RAW TO ROLE "RAW.REPORTING.no_pii.reader.role" ;
GRANT USAGE ON SCHEMA RAW.REPORTING TO ROLE "RAW.REPORTING.no_pii.reader.role" ;

-- CREATE THE INDIVIDUAL TABLE ROLES AND ONE VIEW ROLE
CREATE ROLE "RAW.REPORTING.DRDOWLING_OTHER_PROVIDERS.no_pii.table.reader";

-- ADD THE INVIDUAL ROLES TO THIER TABLES/VIEW
GRANT SELECT ON TABLE "RAW"."REPORTING"."DRDOWLING_OTHER_PROVIDERS" TO ROLE "RAW.REPORTING.DRDOWLING_OTHER_PROVIDERS.no_pii.table.reader";

-- ADD THE INVIDUAL ROLES TO ITS GrOUP ROLES
GRANT ROLE "RAW.REPORTING.DRDOWLING_OTHER_PROVIDERS.no_pii.table.reader" TO ROLE "RAW.REPORTING.no_pii.reader.role" ;

-- ADD GROUP ROLES TO THE DBT ROLE
GRANT ROLE "RAW.REPORTING.no_pii.reader.role"   TO ROLE DBT_DEV;
GRANT ROLE "RAW.REPORTING.no_pii.reader.role"   TO ROLE DBT_PROD;


-------############################################################


-------############################################################

-- TEST AND JUNK STUFF BELOW ----------------------------

------###############################################################

USE WAREHOUSE TRANSFORMING_WAREHOUSE;
USE DATABASE RAW;
USE SCHEMA REPORTING;

show grants on schema REPORTING;
FIVETRAN_ROLE
TRANSFORMER
BUSINESS_INTELLIGENCE

show grants to ROLE FIVETRAN_ROLE ;
show grants ON ROLE FIVETRAN_ROLE;

SHOW GRANTS ON TABLE
"RAW"."REPORTING"."DRDOWLING_OTHER_PROVIDERS"
-- TRANSFORMER HAS SELECTS DIRECTLY ON THE TABLEs