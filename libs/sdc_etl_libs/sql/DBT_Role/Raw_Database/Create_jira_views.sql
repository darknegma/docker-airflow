-- UPDATE PLAN
-- 1 TABLES ARE BEING USED BY DBT
-- 1 NO_PII TABLES - CREATE THE INDIVIDUAL READ ROLES FOR THESE
-- 0 PII TABLE-- 0 VIEWS NEEDED
-- ADD THE  read ROLES TO THE NO_PII GROUP ROLE
-- ADD THE NO_PII GROUP ROLE TO BOTH DBT ROLES

-- RAW.STITCH_JIRA.ISSUES

USE ROLE AIRFLOW_SERVICE_ROLE;
USE DATABASE RAW;
USE SCHEMA STITCH_JIRA;

USE ROLE SECURITYADMIN;

-- CREATE THE GROUP ROLES
CREATE ROLE "RAW.STITCH_JIRA.no_pii.reader.role" ;

-- ADD PERMISIONS
GRANT USAGE ON DATABASE RAW TO ROLE "RAW.STITCH_JIRA.no_pii.reader.role" ;
GRANT USAGE ON SCHEMA RAW.STITCH_JIRA TO ROLE "RAW.STITCH_JIRA.no_pii.reader.role" ;

-- CREATE THE INDIVIDUAL TABLE ROLES AND ONE VIEW ROLE
CREATE ROLE "RAW.STITCH_JIRA.ISSUES.no_pii.table.reader";

-- ADD THE INVIDUAL ROLES TO THIER TABLES/VIEW
GRANT SELECT ON TABLE "RAW"."STITCH_JIRA"."ISSUES" TO ROLE "RAW.STITCH_JIRA.ISSUES.no_pii.table.reader";


-- ADD THE INVIDUAL ROLES TO ITS GrOUP ROLES
GRANT ROLE "RAW.STITCH_JIRA.ISSUES.no_pii.table.reader" TO ROLE "RAW.STITCH_JIRA.no_pii.reader.role"  ;

-- ADD GROUP ROLES TO THE DBT ROLE
GRANT ROLE "RAW.STITCH_JIRA.no_pii.reader.role"     TO ROLE DBT_DEV;
GRANT ROLE "RAW.STITCH_JIRA.no_pii.reader.role"     TO ROLE DBT_PROD;

-------############################################################

-- TEST AND JUNK STUFF BELOW ----------------------------

------###############################################################

USE WAREHOUSE TRANSFORMING_WAREHOUSE;
USE DATABASE RAW;
USE SCHEMA STITCH_JIRA;


show grants on schema STITCH_JIRA;
STITCHROLE
TRANSFORMER

SHOW GRANTS ON TABLE
-- TRANSFORMER HAS OWNERSHIP DIRECTLY ON THE TABLE