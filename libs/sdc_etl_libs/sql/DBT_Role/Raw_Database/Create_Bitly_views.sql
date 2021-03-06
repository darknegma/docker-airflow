-- UPDATE PLAN
-- 3 TABLES ARE BEING USED BY DBT
-- 3 NO_PII TABLES - CREATE THE INDIVIDUAL READ ROLES FOR THESE
-- 0 PII TABLE-- 0 VIEWS NEEDED
-- ADD THE  read ROLES TO THE NO_PII GROUP ROLE
-- ADD THE NO_PII GROUP ROLE TO BOTH DBT ROLES

--RAW.STITCH_BITLY.BITLINKS
--RAW.STITCH_BITLY.CLICKS
--RAW.STITCH_BITLY.GROUP

USE DATABASE RAW;
USE SCHEMA STITCH_BITLY;
USE ROLE AIRFLOW_SERVICE_ROLE;

USE ROLE SECURITYADMIN;

-- CREATE THE GROUP ROLES
CREATE ROLE "RAW.STITCH_BITLY.no_pii.reader.role" ;

-- ADD PERMISIONS
GRANT USAGE ON DATABASE RAW TO ROLE "RAW.STITCH_BITLY.no_pii.reader.role" ;
GRANT USAGE ON SCHEMA RAW.STITCH_BITLY TO ROLE "RAW.STITCH_BITLY.no_pii.reader.role" ;

-- CREATE THE INDIVIDUAL TABLE ROLES AND ONE VIEW ROLE
CREATE ROLE "RAW.STITCH_BITLY.BITLINKS.no_pii.table.reader";
CREATE ROLE "RAW.STITCH_BITLY.CLICKS.no_pii.table.reader";
CREATE ROLE "RAW.STITCH_BITLY.GROUPS.no_pii.table.reader";

-- ADD THE INVIDUAL ROLES TO THIER TABLES/VIEW
GRANT SELECT ON TABLE "RAW"."STITCH_BITLY"."BITLINKS" TO ROLE "RAW.STITCH_BITLY.BITLINKS.no_pii.table.reader";
GRANT SELECT ON TABLE "RAW"."STITCH_BITLY"."CLICKS" TO ROLE "RAW.STITCH_BITLY.CLICKS.no_pii.table.reader";
GRANT SELECT ON TABLE "RAW"."STITCH_BITLY"."GROUPS" TO ROLE "RAW.STITCH_BITLY.GROUPS.no_pii.table.reader";

-- ADD THE INVIDUAL ROLES TO ITS GrOUP ROLES
GRANT ROLE "RAW.STITCH_BITLY.BITLINKS.no_pii.table.reader" TO ROLE "RAW.STITCH_BITLY.no_pii.reader.role" ;
GRANT ROLE "RAW.STITCH_BITLY.CLICKS.no_pii.table.reader" TO ROLE "RAW.STITCH_BITLY.no_pii.reader.role" ;
GRANT ROLE "RAW.STITCH_BITLY.GROUPS.no_pii.table.reader" TO ROLE "RAW.STITCH_BITLY.no_pii.reader.role" ;

-- ADD GROUP ROLES TO THE DBT ROLE
GRANT ROLE "RAW.STITCH_BITLY.no_pii.reader.role"      TO ROLE DBT_DEV;
GRANT ROLE "RAW.STITCH_BITLY.no_pii.reader.role"      TO ROLE DBT_PROD;

-------############################################################

-- TEST AND JUNK STUFF BELOW ----------------------------

------###############################################################

USE WAREHOUSE TRANSFORMING_WAREHOUSE;
USE DATABASE RAW;
USE SCHEMA STITCH_BITLY;


show grants on schema STITCH_BITLY;
STITCHROLE
TRANSFORMER

SHOW GRANTS ON TABLE
-- TRANSFORMER HAS OWNERSHIP DIRECTLY ON THE TABLE