-- UPDATE PLAN
-- 4 TABLES ARE BEING USED BY DBT
-- 2 NO_PII TABLES - CREATE THE INDIVIDUAL READ ROLES FOR THESE
-- 2 PII TABLE-- 2 VIEWS NEEDED
-- ADD THE  read ROLES TO THE NO_PII GROUP ROLE
-- ADD THE NO_PII GROUP ROLE TO BOTH DBT ROLES
"RAW.STITCH_TYPEFORM.JOB.no_pii.table.reader";
--RAW.STITCH_ULTIPRO.AUDIT
--RAW.STITCH_ULTIPRO.EMPLOYEE

--RAW.STITCH_ULTIPRO.JOB
--RAW.STITCH_ULTIPRO.LOCATION

USE ROLE AIRFLOW_SERVICE_ROLE;
USE DATABASE RAW;
USE SCHEMA STITCH_ULTIPRO;

CREATE OR REPLACE VIEW VW_AUDIT COPY GRANTS  AS
(
    SELECT
        EMPLOYEE_NUMBER	,
        CASE WHEN PII_ACCESS = TRUE THEN EMPLOYEE_NAME ELSE sha2(EMPLOYEE_NAME,512) END AS EMPLOYEE_NAME,
        REASON_CODE		,
        REASON		,
        LAST_HIRE_DATE		,
        TERMINATION_DATE		,
        EFFECTIVE_DATE		,
        EJH_DATE_TIME_CREATED		,
        EMPLOYEE_STATUS		,
        COUNTRY_CODE		,
        LANGUAGE_CODE		,
        COMPANY		,
        COMPANY_CODE

  FROM AUDIT
  LEFT JOIN DATAENG_UTILS.MAPPINGS.PII_MAPPINGS PII ON PII.ROLE = CURRENT_ROLE()

);

CREATE OR REPLACE VIEW VW_EMPLOYEE  COPY GRANTS  AS
(
    SELECT
        ACCESSGROUPID	,
        ACTIVE	,
        CASE WHEN PII_ACCESS = TRUE THEN BIRTHDATE ELSE '2099-01-01' END AS BIRTHDATE,
        CARDNUM	,
        CASE WHEN PII_ACCESS = TRUE THEN EMAIL ELSE sha2(EMAIL,512) END AS EMAIL	,
        EMPID	,
        CASE WHEN PII_ACCESS = TRUE THEN FIRSTNAME ELSE sha2(FIRSTNAME,512) END AS FIRSTNAME	,
        HIREDATE	,
        HOLIRULE ,
        ID	,
        JOBID	,
        CASE WHEN PII_ACCESS = TRUE THEN LASTNAME ELSE sha2(LASTNAME,512) END AS LASTNAME,
        LOCATIONID	,
        ORGLEVEL1ID	,
        ORGLEVEL2ID ,
        ORGLEVEL3ID	,
        ORGLEVEL4ID,
        PAYCATE	,
        PAYMETHOD	,
        PAYPOLICYID,
        PAYTYPE	,
        PROJECTID	,
        SHIFTID	,
        SUPID	,
         CASE WHEN PII_ACCESS = TRUE THEN SUPNAME ELSE sha2(SUPNAME,512) END AS SUPNAME	,
        _SDC_BATCHED_AT	 ,
        _SDC_RECEIVED_AT	,
        _SDC_SEQUENCE ,
        _SDC_TABLE_VERSION

  FROM EMPLOYEE
  LEFT JOIN DATAENG_UTILS.MAPPINGS.PII_MAPPINGS PII ON PII.ROLE = CURRENT_ROLE()

);


USE ROLE SECURITYADMIN;

-- CREATE THE GROUP ROLES
CREATE ROLE "RAW.STITCH_ULTIPRO.no_pii.reader.role" ;

-- ADD PERMISIONS
GRANT USAGE ON DATABASE RAW TO ROLE "RAW.STITCH_ULTIPRO.no_pii.reader.role" ;
GRANT USAGE ON SCHEMA RAW.STITCH_ULTIPRO TO ROLE "RAW.STITCH_ULTIPRO.no_pii.reader.role" ;

-- CREATE THE INDIVIDUAL TABLE ROLES AND VIEW ROLE
CREATE ROLE "RAW.STITCH_ULTIPRO.AUDIT.no_pii.view.reader";
CREATE ROLE "RAW.STITCH_ULTIPRO.EMPLOYEE.no_pii.view.reader";

CREATE ROLE "RAW.STITCH_ULTIPRO.JOB.no_pii.table.reader";
CREATE ROLE "RAW.STITCH_ULTIPRO.LOCATION.no_pii.table.reader";

-- ADD THE INVIDUAL ROLES TO THIER TABLES/VIEW
GRANT SELECT ON VIEW "RAW"."STITCH_ULTIPRO"."VW_AUDIT" TO ROLE "RAW.STITCH_ULTIPRO.AUDIT.no_pii.view.reader";
GRANT SELECT ON VIEW "RAW"."STITCH_ULTIPRO"."VW_EMPLOYEE" TO ROLE "RAW.STITCH_ULTIPRO.EMPLOYEE.no_pii.view.reader";

GRANT SELECT ON TABLE "RAW"."STITCH_ULTIPRO"."JOB" TO ROLE "RAW.STITCH_ULTIPRO.JOB.no_pii.table.reader";
GRANT SELECT ON TABLE "RAW"."STITCH_ULTIPRO"."LOCATION" TO ROLE "RAW.STITCH_ULTIPRO.LOCATION.no_pii.table.reader";

-- ADD THE INVIDUAL ROLES TO ITS GrOUP ROLES
GRANT ROLE "RAW.STITCH_ULTIPRO.AUDIT.no_pii.view.reader" TO ROLE  "RAW.STITCH_ULTIPRO.no_pii.reader.role" ;
GRANT ROLE "RAW.STITCH_ULTIPRO.EMPLOYEE.no_pii.view.reader" TO ROLE  "RAW.STITCH_ULTIPRO.no_pii.reader.role" ;
GRANT ROLE "RAW.STITCH_ULTIPRO.JOB.no_pii.table.reader" TO ROLE  "RAW.STITCH_ULTIPRO.no_pii.reader.role" ;
GRANT ROLE "RAW.STITCH_ULTIPRO.LOCATION.no_pii.table.reader" TO ROLE  "RAW.STITCH_ULTIPRO.no_pii.reader.role" ;

-- ADD GROUP ROLES TO THE DBT ROLE
GRANT ROLE "RAW.STITCH_ULTIPRO.no_pii.reader.role"   TO ROLE DBT_DEV;
GRANT ROLE "RAW.STITCH_ULTIPRO.no_pii.reader.role"   TO ROLE DBT_PROD;


-------############################################################


-------############################################################

-- TEST AND JUNK STUFF BELOW ----------------------------

------###############################################################

USE WAREHOUSE TRANSFORMING_WAREHOUSE;
USE DATABASE RAW;
USE SCHEMA STITCH_ULTIPRO;

show grants on schema STITCH_ULTIPRO;
STITCH_ROLE
TRANSFORMER


SHOW GRANTS ON TABLE "RAW"."STITCH_ULTIPRO"."AUDIT"
--TRANSFORMER - FULL RIGHTS ON TABLE
SHOW GRANTS ON TABLE "RAW"."STITCH_ULTIPRO"."EMPLOYEE"
--TRANSFORMER - FULL RIGHTS ON TABLE