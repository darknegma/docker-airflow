-- UPDATE PLAN
-- 8 TABLES ARE BEING USED BY DBT
-- 8 NO_PII TABLES - CREATE THE INDIVIDUAL READ ROLES FOR THESE
-- 0 PII TABLE-- 0 VIEWS NEEDED
-- ADD THE  read ROLES TO THE NO_PII GROUP ROLE
-- ADD THE NO_PII GROUP ROLE TO BOTH DBT ROLES


--RAW.PRINT_FILE.ACTIVE_JOBS_DAILY_LOG
--RAW.PRINT_FILE.BATCH_JOBS_DAILY_LOG
--RAW.PRINT_FILE.CANCELED_CASES_DAILY_LOG
--RAW.PRINT_FILE.COMPLETED_JOBS_DAILY_LOG
--RAW.PRINT_FILE.PRINT_FILES_DAILY_LOG
--RAW.PRINT_FILE.QA_DAILY_LOG
--RAW.PRINT_FILE.RETAINER_DAILY_LOG
--RAW.PRINT_FILE.SORTLOG

USE ROLE AIRFLOW_SERVICE_ROLE;
USE DATABASE RAW;
USE SCHEMA PRINT_FILE;


USE ROLE SECURITYADMIN;

-- CREATE THE GROUP ROLES
CREATE ROLE "RAW.PRINT_FILE.no_pii.reader.role" ;

-- ADD PERMISIONS
GRANT USAGE ON DATABASE RAW TO ROLE "RAW.PRINT_FILE.no_pii.reader.role" ;
GRANT USAGE ON SCHEMA RAW.PRINT_FILE TO ROLE "RAW.PRINT_FILE.no_pii.reader.role" ;

-- CREATE THE INDIVIDUAL TABLE ROLES AND ONE VIEW ROLE
CREATE ROLE "RAW.PRINT_FILE.ACTIVE_JOBS_DAILY_LOG.no_pii.table.reader";
CREATE ROLE "RAW.PRINT_FILE.BATCH_JOBS_DAILY_LOG.no_pii.table.reader";
CREATE ROLE "RAW.PRINT_FILE.CANCELED_CASES_DAILY_LOG.no_pii.table.reader";
CREATE ROLE "RAW.PRINT_FILE.COMPLETED_JOBS_DAILY_LOG.no_pii.table.reader";
CREATE ROLE "RAW.PRINT_FILE.PRINT_FILES_DAILY_LOG.no_pii.table.reader";
CREATE ROLE "RAW.PRINT_FILE.QA_DAILY_LOG.no_pii.table.reader";
CREATE ROLE "RAW.PRINT_FILE.RETAINER_DAILY_LOG.no_pii.table.reader";
CREATE ROLE "RAW.PRINT_FILE.SORTLOG.no_pii.table.reader";

-- ADD THE INVIDUAL ROLES TO THIER TABLES/VIEWS
GRANT SELECT ON TABLE "RAW"."PRINT_FILE"."ACTIVE_JOBS_DAILY_LOG" TO ROLE "RAW.PRINT_FILE.ACTIVE_JOBS_DAILY_LOG.no_pii.table.reader";
GRANT SELECT ON TABLE "RAW"."PRINT_FILE"."BATCH_JOBS_DAILY_LOG" TO ROLE "RAW.PRINT_FILE.BATCH_JOBS_DAILY_LOG.no_pii.table.reader";
GRANT SELECT ON TABLE "RAW"."PRINT_FILE"."CANCELED_CASES_DAILY_LOG" TO ROLE "RAW.PRINT_FILE.CANCELED_CASES_DAILY_LOG.no_pii.table.reader";
GRANT SELECT ON TABLE "RAW"."PRINT_FILE"."COMPLETED_JOBS_DAILY_LOG" TO ROLE "RAW.PRINT_FILE.COMPLETED_JOBS_DAILY_LOG.no_pii.table.reader";
GRANT SELECT ON TABLE "RAW"."PRINT_FILE"."PRINT_FILES_DAILY_LOG" TO ROLE "RAW.PRINT_FILE.PRINT_FILES_DAILY_LOG.no_pii.table.reader";
GRANT SELECT ON TABLE "RAW"."PRINT_FILE"."QA_DAILY_LOG" TO ROLE "RAW.PRINT_FILE.QA_DAILY_LOG.no_pii.table.reader";
GRANT SELECT ON TABLE "RAW"."PRINT_FILE"."RETAINER_DAILY_LOG" TO ROLE "RAW.PRINT_FILE.RETAINER_DAILY_LOG.no_pii.table.reader";
GRANT SELECT ON TABLE "RAW"."PRINT_FILE"."SORTLOG" TO ROLE "RAW.PRINT_FILE.SORTLOG.no_pii.table.reader";

-- ADD THE INVIDUAL ROLES TO ITS GrOUP ROLES
GRANT ROLE "RAW.PRINT_FILE.ACTIVE_JOBS_DAILY_LOG.no_pii.table.reader" TO ROLE "RAW.PRINT_FILE.no_pii.reader.role" ;
GRANT ROLE "RAW.PRINT_FILE.BATCH_JOBS_DAILY_LOG.no_pii.table.reader" TO ROLE "RAW.PRINT_FILE.no_pii.reader.role" ;
GRANT ROLE "RAW.PRINT_FILE.CANCELED_CASES_DAILY_LOG.no_pii.table.reader" TO ROLE "RAW.PRINT_FILE.no_pii.reader.role" ;
GRANT ROLE "RAW.PRINT_FILE.COMPLETED_JOBS_DAILY_LOG.no_pii.table.reader" TO ROLE "RAW.PRINT_FILE.no_pii.reader.role" ;
GRANT ROLE "RAW.PRINT_FILE.PRINT_FILES_DAILY_LOG.no_pii.table.reader" TO ROLE "RAW.PRINT_FILE.no_pii.reader.role" ;
GRANT ROLE "RAW.PRINT_FILE.QA_DAILY_LOG.no_pii.table.reader" TO ROLE "RAW.PRINT_FILE.no_pii.reader.role" ;
GRANT ROLE "RAW.PRINT_FILE.RETAINER_DAILY_LOG.no_pii.table.reader"TO ROLE "RAW.PRINT_FILE.no_pii.reader.role" ;
GRANT ROLE "RAW.PRINT_FILE.SORTLOG.no_pii.table.reader" TO ROLE "RAW.PRINT_FILE.no_pii.reader.role" ;

-- ADD GROUP ROLES TO THE DBT ROLE
GRANT ROLE "RAW.PRINT_FILE.no_pii.reader.role"    TO ROLE DBT_DEV;
GRANT ROLE "RAW.PRINT_FILE.no_pii.reader.role"    TO ROLE DBT_PROD;


-------############################################################


-------############################################################

-- TEST AND JUNK STUFF BELOW ----------------------------

------###############################################################

USE WAREHOUSE TRANSFORMING_WAREHOUSE;
USE DATABASE RAW;
USE SCHEMA PRINT_FILE;

show grants on schema PRINT_FILE;
FIVETRAN_ROLE
TRANSFORMER
AD_HOC


show grants to ROLE FIVETRAN_ROLE ;
show grants ON ROLE FIVETRAN_ROLE;

SHOW GRANTS ON TABLE
-- TRANSFORMER HAS SELECTS deletes and updates DIRECTLY ON THE TABLEs