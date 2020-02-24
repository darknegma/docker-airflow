-- UPDATE PLAN
-- 2 TABLES ARE BEING USED BY DBT
-- 2 NO_PII TABLES - CREATE THE INDIVIDUAL READ ROLES FOR THESE
-- 0 PII TABLE-- 0 VIEWS NEEDED
-- ADD THE  read ROLES TO THE NO_PII GROUP ROLE
-- ADD THE NO_PII GROUP ROLE TO BOTH DBT ROLES

-- RAW.LABOR_HOURS.LABOR_HOURS_BY_WEEK_BY_GUIDE
-- RAW.LABOR_HOURS.SATURDAY_LABOR_HOURS

USE ROLE AIRFLOW_SERVICE_ROLE;
USE DATABASE RAW;
USE SCHEMA LABOR_HOURS;

USE ROLE SECURITYADMIN;

-- CREATE THE GROUP ROLES
CREATE ROLE "RAW.LABOR_HOURS.no_pii.reader.role" ;

-- ADD PERMISIONS
GRANT USAGE ON DATABASE RAW TO ROLE "RAW.LABOR_HOURS.no_pii.reader.role" ;
GRANT USAGE ON SCHEMA RAW.LABOR_HOURS TO ROLE "RAW.LABOR_HOURS.no_pii.reader.role" ;

-- CREATE THE INDIVIDUAL TABLE ROLES AND ONE VIEW ROLE
CREATE ROLE "RAW.LABOR_HOURS.LABOR_HOURS_BY_WEEK_BY_GUIDE.no_pii.table.reader";
CREATE ROLE "RAW.LABOR_HOURS.SATURDAY_LABOR_HOURS.no_pii.table.reader";

-- ADD THE INVIDUAL ROLES TO THIER TABLES/VIEW
GRANT SELECT ON TABLE "RAW"."LABOR_HOURS"."LABOR_HOURS_BY_WEEK_BY_GUIDE" TO ROLE "RAW.LABOR_HOURS.LABOR_HOURS_BY_WEEK_BY_GUIDE.no_pii.table.reader";
GRANT SELECT ON TABLE "RAW"."LABOR_HOURS"."SATURDAY_LABOR_HOURS" TO ROLE "RAW.LABOR_HOURS.SATURDAY_LABOR_HOURS.no_pii.table.reader";

-- ADD THE INVIDUAL ROLES TO ITS GrOUP ROLES
GRANT ROLE "RAW.LABOR_HOURS.LABOR_HOURS_BY_WEEK_BY_GUIDE.no_pii.table.reader" TO ROLE "RAW.LABOR_HOURS.no_pii.reader.role";
GRANT ROLE "RAW.LABOR_HOURS.SATURDAY_LABOR_HOURS.no_pii.table.reader" TO ROLE "RAW.LABOR_HOURS.no_pii.reader.role";


-- ADD GROUP ROLES TO THE DBT ROLE
GRANT ROLE "RAW.LABOR_HOURS.no_pii.reader.role"   TO ROLE DBT_DEV;
GRANT ROLE "RAW.LABOR_HOURS.no_pii.reader.role"   TO ROLE DBT_PROD;


-------############################################################


-------############################################################

-- TEST AND JUNK STUFF BELOW ----------------------------

------###############################################################

USE WAREHOUSE TRANSFORMING_WAREHOUSE;
USE DATABASE RAW;
USE SCHEMA LABOR_HOURS;

show grants on schema LABOR_HOURS;
FIVETRAN_ROLE
TRANSFORMER


show grants to ROLE FIVETRAN_ROLE ;
show grants ON ROLE FIVETRAN_ROLE;

SHOW GRANTS ON TABLE
-- TRANSFORMER HAS SELECTS DIRECTLY ON THE TABLEs