-- UPDATE PLAN
-- 5 TABLES ARE BEING USED BY DBT
-- 5 NO_PII TABLES - CREATE THE INDIVIDUAL READ ROLES FOR THESE
-- 0 PII TABLE-- 0 VIEWS NEEDED
-- ADD THE  read ROLES TO THE NO_PII GROUP ROLE
-- ADD THE NO_PII GROUP ROLE TO BOTH DBT ROLES

--RAW.FIVETRAN_YAHOO_ADS.AD_GROUP_HISTORY
--RAW.FIVETRAN_YAHOO_ADS.ADVERTISER_HISTORY
--RAW.FIVETRAN_YAHOO_ADS.CAMPAIGN_HISTORY
--RAW.FIVETRAN_YAHOO_ADS.PERFORMANCE_STATS_DAILY_REPORT



USE DATABASE RAW;
USE SCHEMA FIVETRAN_YAHOO_ADS;


USE ROLE SECURITYADMIN;

-- CREATE THE GROUP ROLES
CREATE ROLE "RAW.FIVETRAN_YAHOO_ADS.no_pii.reader.role" ;

-- ADD PERMISIONS
GRANT USAGE ON DATABASE RAW TO ROLE "RAW.FIVETRAN_YAHOO_ADS.no_pii.reader.role" ;
GRANT USAGE ON SCHEMA RAW.FIVETRAN_YAHOO_ADS TO ROLE "RAW.FIVETRAN_YAHOO_ADS.no_pii.reader.role" ;

-- CREATE THE INDIVIDUAL TABLE ROLES AND ONE VIEW ROLE
CREATE ROLE "RAW.FIVETRAN_YAHOO_ADS.AD_GROUP_HISTORY.no_pii.table.reader";
CREATE ROLE "RAW.FIVETRAN_YAHOO_ADS.ADVERTISER_HISTORY.no_pii.table.reader";
CREATE ROLE "RAW.FIVETRAN_YAHOO_ADS.CAMPAIGN_HISTORY.no_pii.table.reader";
CREATE ROLE "RAW.FIVETRAN_YAHOO_ADS.PERFORMANCE_STATS_DAILY_REPORT.no_pii.table.reader";

-- ADD THE INVIDUAL ROLES TO THIER TABLES/VIEW
GRANT SELECT ON TABLE "RAW"."FIVETRAN_YAHOO_ADS"."AD_GROUP_HISTORY" TO ROLE  "RAW.FIVETRAN_YAHOO_ADS.AD_GROUP_HISTORY.no_pii.table.reader";
GRANT SELECT ON TABLE "RAW"."FIVETRAN_YAHOO_ADS"."ADVERTISER_HISTORY" TO ROLE "RAW.FIVETRAN_YAHOO_ADS.ADVERTISER_HISTORY.no_pii.table.reader";
GRANT SELECT ON TABLE "RAW"."FIVETRAN_YAHOO_ADS"."CAMPAIGN_HISTORY" TO ROLE "RAW.FIVETRAN_YAHOO_ADS.CAMPAIGN_HISTORY.no_pii.table.reader";
GRANT SELECT ON TABLE "RAW"."FIVETRAN_YAHOO_ADS"."PERFORMANCE_STATS_DAILY_REPORT" TO ROLE "RAW.FIVETRAN_YAHOO_ADS.PERFORMANCE_STATS_DAILY_REPORT.no_pii.table.reader";

-- ADD THE INVIDUAL ROLES TO ITS GrOUP ROLES
GRANT ROLE "RAW.FIVETRAN_YAHOO_ADS.AD_GROUP_HISTORY.no_pii.table.reader" TO ROLE  "RAW.FIVETRAN_YAHOO_ADS.no_pii.reader.role" ;
GRANT ROLE "RAW.FIVETRAN_YAHOO_ADS.ADVERTISER_HISTORY.no_pii.table.reader" TO ROLE  "RAW.FIVETRAN_YAHOO_ADS.no_pii.reader.role" ;
GRANT ROLE "RAW.FIVETRAN_YAHOO_ADS.CAMPAIGN_HISTORY.no_pii.table.reader" TO ROLE  "RAW.FIVETRAN_YAHOO_ADS.no_pii.reader.role" ;
GRANT ROLE "RAW.FIVETRAN_YAHOO_ADS.PERFORMANCE_STATS_DAILY_REPORT.no_pii.table.reader" TO ROLE  "RAW.FIVETRAN_YAHOO_ADS.no_pii.reader.role" ;

-- ADD GROUP ROLES TO THE DBT ROLE
GRANT ROLE "RAW.FIVETRAN_YAHOO_ADS.no_pii.reader.role"   TO ROLE DBT_DEV;
GRANT ROLE "RAW.FIVETRAN_YAHOO_ADS.no_pii.reader.role"   TO ROLE DBT_PROD;


-------############################################################

--RAW.FIVETRAN_YAHOO.YAHOO_ADS


USE DATABASE RAW;
USE SCHEMA FIVETRAN_YAHOO;


USE ROLE SECURITYADMIN;

-- CREATE THE GROUP ROLES
CREATE ROLE "RAW.FIVETRAN_YAHOO.no_pii.reader.role" ;

-- ADD PERMISIONS
GRANT USAGE ON DATABASE RAW TO ROLE "RAW.FIVETRAN_YAHOO.no_pii.reader.role" ;
GRANT USAGE ON SCHEMA RAW.FIVETRAN_YAHOO TO ROLE "RAW.FIVETRAN_YAHOO.no_pii.reader.role" ;

-- CREATE THE INDIVIDUAL TABLE ROLES AND ONE VIEW ROLE
CREATE ROLE "RAW.FIVETRAN_YAHOO.YAHOO_ADS.no_pii.table.reader";

-- ADD THE INVIDUAL ROLES TO THIER TABLES/VIEW
GRANT SELECT ON TABLE "RAW"."FIVETRAN_YAHOO_ADS"."YAHOO_ADS" TO ROLE  "RAW.FIVETRAN_YAHOO.YAHOO_ADS.no_pii.table.reader";

-- ADD THE INVIDUAL ROLES TO ITS GrOUP ROLES
GRANT SELECT ON TABLE "RAW"."FIVETRAN_YAHOO"."YAHOO_ADS" TO ROLE  "RAW.FIVETRAN_YAHOO.YAHOO_ADS.no_pii.table.reader";

-- ADD GROUP ROLES TO THE DBT ROLE
GRANT ROLE "RAW.FIVETRAN_YAHOO.no_pii.reader.role"  TO ROLE DBT_DEV;
GRANT ROLE "RAW.FIVETRAN_YAHOO.no_pii.reader.role"   TO ROLE DBT_PROD;


-------############################################################

-- TEST AND JUNK STUFF BELOW ----------------------------

------###############################################################

USE WAREHOUSE TRANSFORMING_WAREHOUSE;
USE DATABASE RAW;
USE SCHEMA FIVETRAN_YAHOO_ADS;

show grants on schema FIVETRAN_YAHOO_ADS;
FIVETRAN_ROLE
TRANSFORMER


show grants to ROLE FIVETRAN_ROLE ;
show grants ON ROLE FIVETRAN_ROLE;

SHOW GRANTS ON TABLE
-- TRANSFORMER HAS SELECTS DIRECTLY ON THE TABLEs