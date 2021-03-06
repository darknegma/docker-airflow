-- UPDATE PLAN
-- 6 TABLES ARE BEING USED BY DBT
-- 6 NO_PII TABLES - CREATE THE INDIVIDUAL READ ROLES FOR THESE
-- 0 PII TABLE-- 0 VIEWS NEEDED
-- ADD THE  read ROLES TO THE NO_PII GROUP ROLE
-- ADD THE NO_PII GROUP ROLE TO BOTH DBT ROLES

--RAW.ZIP_CODE_DATA.ALL_ZIP_CODES
--RAW.ZIP_CODE_DATA.AUSTRALIA_POSTALCODE_POPULATION_INCOME
--RAW.ZIP_CODE_DATA.CANADIAN_FSA_INFORMATION
--RAW.ZIP_CODE_DATA.CANADIAN_POSTAL_CODES
--RAW.ZIP_CODE_DATA.STATE_ABBREVIATIONS
--RAW.ZIP_CODE_DATA.ZIP_CODE_TO_DMA

USE ROLE AIRFLOW_SERVICE_ROLE;
USE DATABASE RAW;
USE SCHEMA ZIP_CODE_DATA;

USE ROLE SECURITYADMIN;

-- CREATE THE GROUP ROLES
CREATE ROLE "RAW.ZIP_CODE_DATA.no_pii.reader.role" ;

-- ADD PERMISIONS
GRANT USAGE ON DATABASE RAW TO ROLE "RAW.ZIP_CODE_DATA.no_pii.reader.role" ;
GRANT USAGE ON SCHEMA RAW.ZIP_CODE_DATA TO ROLE "RAW.ZIP_CODE_DATA.no_pii.reader.role" ;

-- CREATE THE INDIVIDUAL TABLE ROLES AND ONE VIEW ROLE
CREATE ROLE "RAW.ZIP_CODE_DATA.ALL_ZIP_CODES.no_pii.table.reader";
CREATE ROLE "RAW.ZIP_CODE_DATA.AUSTRALIA_POSTALCODE_POPULATION_INCOME.no_pii.table.reader";
CREATE ROLE "RAW.ZIP_CODE_DATA.CANADIAN_FSA_INFORMATION.no_pii.table.reader";
CREATE ROLE "RAW.ZIP_CODE_DATA.CANADIAN_POSTAL_CODES.no_pii.table.reader";
CREATE ROLE "RAW.ZIP_CODE_DATA.STATE_ABBREVIATIONS.no_pii.table.reader";
CREATE ROLE "RAW.ZIP_CODE_DATA.ZIP_CODE_TO_DMA.no_pii.table.reader";

-- ADD THE INVIDUAL ROLES TO THIER TABLES/VIE
GRANT SELECT ON TABLE "RAW"."ZIP_CODE_DATA"."ALL_ZIP_CODES" TO ROLE "RAW.ZIP_CODE_DATA.ALL_ZIP_CODES.no_pii.table.reader";
GRANT SELECT ON TABLE "RAW"."ZIP_CODE_DATA"."AUSTRALIA_POSTALCODE_POPULATION_INCOME" TO ROLE "RAW.ZIP_CODE_DATA.AUSTRALIA_POSTALCODE_POPULATION_INCOME.no_pii.table.reader";
GRANT SELECT ON TABLE "RAW"."ZIP_CODE_DATA"."CANADIAN_FSA_INFORMATION" TO ROLE "RAW.ZIP_CODE_DATA.CANADIAN_FSA_INFORMATION.no_pii.table.reader";
GRANT SELECT ON TABLE "RAW"."ZIP_CODE_DATA"."CANADIAN_POSTAL_CODES" TO ROLE "RAW.ZIP_CODE_DATA.CANADIAN_POSTAL_CODES.no_pii.table.reader";
GRANT SELECT ON TABLE "RAW"."ZIP_CODE_DATA"."STATE_ABBREVIATIONS" TO ROLE "RAW.ZIP_CODE_DATA.STATE_ABBREVIATIONS.no_pii.table.reader";
GRANT SELECT ON TABLE "RAW"."ZIP_CODE_DATA"."ZIP_CODE_TO_DMA" TO ROLE "RAW.ZIP_CODE_DATA.ZIP_CODE_TO_DMA.no_pii.table.reader";


-- ADD THE INVIDUAL ROLES TO ITS GrOUP ROLES
GRANT ROLE "RAW.ZIP_CODE_DATA.ALL_ZIP_CODES.no_pii.table.reader" TO ROLE  "RAW.ZIP_CODE_DATA.no_pii.reader.role" ;
GRANT ROLE "RAW.ZIP_CODE_DATA.AUSTRALIA_POSTALCODE_POPULATION_INCOME.no_pii.table.reader" TO ROLE "RAW.ZIP_CODE_DATA.no_pii.reader.role" ;
GRANT ROLE "RAW.ZIP_CODE_DATA.CANADIAN_FSA_INFORMATION.no_pii.table.reader" TO ROLE "RAW.ZIP_CODE_DATA.no_pii.reader.role" ;
GRANT ROLE "RAW.ZIP_CODE_DATA.CANADIAN_POSTAL_CODES.no_pii.table.reader" TO ROLE  "RAW.ZIP_CODE_DATA.no_pii.reader.role" ;
GRANT ROLE "RAW.ZIP_CODE_DATA.STATE_ABBREVIATIONS.no_pii.table.reader" TO ROLE "RAW.ZIP_CODE_DATA.no_pii.reader.role" ;
GRANT ROLE "RAW.ZIP_CODE_DATA.ZIP_CODE_TO_DMA.no_pii.table.reader" TO ROLE "RAW.ZIP_CODE_DATA.no_pii.reader.role" ;

-- ADD GROUP ROLES TO THE DBT ROLE
GRANT ROLE "RAW.ZIP_CODE_DATA.no_pii.reader.role"    TO ROLE DBT_DEV;
GRANT ROLE "RAW.ZIP_CODE_DATA.no_pii.reader.role"    TO ROLE DBT_PROD;


-------############################################################


-------############################################################

-- TEST AND JUNK STUFF BELOW ----------------------------

------###############################################################

USE WAREHOUSE TRANSFORMING_WAREHOUSE;
USE DATABASE RAW;
USE SCHEMA STITCH_TYPEFORM;

show grants on schema STITCH_TYPEFORM;
STITCH_ROLE
TRANSFORMER


show grants to ROLE FIVETRAN_ROLE ;
show grants ON ROLE FIVETRAN_ROLE;

SHOW GRANTS ON TABLE
"RAW"."STITCH_NETSUITE"."NETSUITE_TRANSACTION"
-- TRANSFORMER HAS OWNE