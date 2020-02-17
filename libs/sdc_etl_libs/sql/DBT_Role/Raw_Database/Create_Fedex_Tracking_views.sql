-- UPDATE PLAN
-- 1 TABLES ARE BEING USED BY DBT
-- 0 NO_PII TABLES - CREATE THE INDIVIDUAL READ ROLES FOR THESE
-- 1 PII TABLE-- CREATE THE 1 VIEWS FOR THE PII TABLE. CREATE A READ ROLE FOR IT
-- ADD THE 1 VIEW ROLES TO THE NO_PII GROUP ROLE
-- ADD THE NO_PII GROUP ROLE TO BOTH DBT ROLES

--RAW.FEDEX.TRACKING_DATA

USE ROLE AIRFLOW_SERVICE_ROLE;
USE DATABASE RAW;
USE SCHEMA FEDEX;

CREATE OR REPLACE VIEW VW_TRACKING_DATA  COPY GRANTS  AS
(
  SELECT
        RTYPE ,
        CCODE ,
        TPIDC	,
        "TRCK#"	,
        FILL1	,
        "MTRK#"	,
        FILL2,
        SHIPD	,
        ESTDD	,
        ESTDT	,
        DELVD ,
        DELVT	,
        PODNM	,
        OCODE	,
        DCODE	,
        STATD	,
        STATC	,
        FILL3	,
        SHPRN	,
        SHPCO	,
        SHPA1	,
        SHPA2	,
        SHPA3	,
        SHPRC	,
        SHPRS	,
        SHPCC	,
        SHPRZ	,
        "ACCT#"	,
        SIREF	,
        CASE WHEN PII_ACCESS = TRUE THEN RCPTN ELSE  sha2(RCPTN,512) END AS RCPTN,
        CASE WHEN PII_ACCESS = TRUE THEN RCPCO ELSE  sha2(RCPCO,512) END AS RCPCO,
        CASE WHEN PII_ACCESS = TRUE THEN RCPA1 ELSE  sha2(RCPA1,512) END AS RCPA1,
        CASE WHEN PII_ACCESS = TRUE THEN RCPA2 ELSE  sha2(RCPA2,512) END AS RCPA2,
        RCPA3	,
        CASE WHEN PII_ACCESS = TRUE THEN RCPTC ELSE  sha2(RCPTC,512) END AS RCPTC,
        RCPTS	,
        RCPTZ	,
        RCPCC	,
        FILL4	,
        SVCCD	,
        PKGCD	,
        TRPAY	,
        DTPAY	,
        TYPCD	,
        FILL5   ,
        PIECS	,
        UOMCD	,
        DIMCD	,
        FILL6	,
        PKGLN	,
        PKGWD	,
        PKGHT	,
        POREF	,
        INREF	,
        "DEPT#"	,
        SHPID	,
        LBWGT	,
        KGWGT	,
        DEXCD	,
        SCODE	,
        "TCN#"	,
        "BOL#"	,
        "PC#1"	,
        "PC#2"	,
        "RMA#"	,
        APPTD	,
        APPTT	,
        EVEST	,
        EVECO	,
        CDRC1	,
        CDRC2	,
        AINFO	,
        SPHC1	,
        SPHC2	,
        SPHC3	,
        SPHC4	,
        "RCPT#"	,
        FILL7

  FROM TRACKING_DATA
  LEFT JOIN DATAENG_UTILS.MAPPINGS.PII_MAPPINGS PII ON PII.ROLE = CURRENT_ROLE()
);

USE ROLE SECURITYADMIN;

-- CREATE THE GROUP ROLES
CREATE ROLE "RAW.FEDEX.no_pii.reader.role" ;

-- ADD PERMISIONS
GRANT USAGE ON DATABASE RAW TO ROLE "RAW.FEDEX.no_pii.reader.role" ;
GRANT USAGE ON SCHEMA RAW.FEDEX TO ROLE "RAW.FEDEX.no_pii.reader.role" ;

-- CREATE THE INDIVIDUAL TABLE ROLES AND ONE VIEW ROLE
CREATE ROLE "RAW.FEDEX.TRACKING_DATA.no_pii.view.reader";

-- ADD THE INVIDUAL ROLES TO THIER TABLES/VIEW
GRANT SELECT ON VIEW "RAW"."FEDEX"."VW_TRACKING_DATA" TO ROLE  "RAW.FEDEX.TRACKING_DATA.no_pii.view.reader";

-- ADD THE INVIDUAL ROLES TO ITS GrOUP ROLES
GRANT ROLE  "RAW.FEDEX.TRACKING_DATA.no_pii.view.reader" TO ROLE "RAW.FEDEX.no_pii.reader.role" ;

-- ADD GROUP ROLES TO THE DBT ROLE
GRANT ROLE "RAW.FEDEX.no_pii.reader.role"   TO ROLE DBT_DEV;
GRANT ROLE "RAW.FEDEX.no_pii.reader.role"   TO ROLE DBT_PROD;

-------############################################################

-- TEST AND JUNK STUFF BELOW ----------------------------

------###############################################################

USE WAREHOUSE TRANSFORMING_WAREHOUSE;
USE DATABASE RAW;
USE SCHEMA FEDEX;

show grants on schema FEDEX;
AWSGLUE_ROLE -- HAS THE TABLE SELECTS



show grants to ROLE AWSGLUE_ROLE ;
show grants ON ROLE AWSGLUE_ROLE;
TRANSFORMER
