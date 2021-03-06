-- UPDATE PLAN
-- 3 TABLES ARE BEING USED BY DBT
-- 2 NO_PII TABLES - CREATE THE INDIVIDUAL READ ROLES FOR THESE
-- 1 PII TABLE-- 1 VIEWS NEEDED
-- ADD THE  read ROLES TO THE NO_PII GROUP ROLE
-- ADD THE NO_PII GROUP ROLE TO BOTH DBT ROLES

--RAW.STITCH_TYPEFORM.ANSWERS
--RAW.STITCH_TYPEFORM.QUESTIONS

--RAW.STITCH_TYPEFORM.LANDINGS

USE ROLE AIRFLOW_SERVICE_ROLE;
USE DATABASE RAW;
USE SCHEMA STITCH_TYPEFORM;

CREATE OR REPLACE VIEW VW_LANDINGS  COPY GRANTS  AS
(
    SELECT
        BROWSER	,
        CASE WHEN PII_ACCESS = TRUE THEN HIDDEN
             ELSE
                REPLACE (
                            PARSE_JSON(HIDDEN), PARSE_JSON(HIDDEN):email, sha2(PARSE_JSON(HIDDEN):email,512)
                        )
        END AS HIDDEN,
        LANDED_AT	,
        LANDING_ID	,
        NETWORK_ID	,
        PLATFORM	,
        CASE WHEN PII_ACCESS = TRUE THEN REFERER ELSE sha2(REFERER,512) END AS  REFERER,
        SUBMITTED_AT	,
        TOKEN	,
        USER_AGENT	,
        _SDC_BATCHED_AT	,
        _SDC_EXTRACTED_AT	,
        _SDC_RECEIVED_AT	,
        _SDC_SEQUENCE	,
        _SDC_TABLE_VERSION

  FROM LANDINGS
  LEFT JOIN DATAENG_UTILS.MAPPINGS.PII_MAPPINGS PII ON PII.ROLE = CURRENT_ROLE()

);


USE ROLE SECURITYADMIN;

-- CREATE THE GROUP ROLES
CREATE ROLE "RAW.STITCH_TYPEFORM.no_pii.reader.role" ;

-- ADD PERMISIONS
GRANT USAGE ON DATABASE RAW TO ROLE "RAW.STITCH_TYPEFORM.no_pii.reader.role" ;
GRANT USAGE ON SCHEMA RAW.STITCH_TYPEFORM TO ROLE "RAW.STITCH_TYPEFORM.no_pii.reader.role" ;

-- CREATE THE INDIVIDUAL TABLE ROLES AND ONE VIEW ROLE
CREATE ROLE "RAW.STITCH_TYPEFORM.LANDINGS.no_pii.view.reader";

CREATE ROLE "RAW.STITCH_TYPEFORM.ANSWERS.no_pii.table.reader";
CREATE ROLE "RAW.STITCH_TYPEFORM.QUESTIONS.no_pii.table.reader";

-- ADD THE INVIDUAL ROLES TO THIER TABLES/VIEW
GRANT SELECT ON VIEW "RAW"."STITCH_TYPEFORM"."VW_LANDINGS" TO ROLE "RAW.STITCH_TYPEFORM.LANDINGS.no_pii.view.reader";

GRANT SELECT ON TABLE "RAW"."STITCH_TYPEFORM"."ANSWERS" TO ROLE "RAW.STITCH_TYPEFORM.ANSWERS.no_pii.table.reader";
GRANT SELECT ON TABLE "RAW"."STITCH_TYPEFORM"."QUESTIONS" TO ROLE "RAW.STITCH_TYPEFORM.QUESTIONS.no_pii.table.reader";

-- ADD THE INVIDUAL ROLES TO ITS GrOUP ROLES
GRANT ROLE "RAW.STITCH_TYPEFORM.LANDINGS.no_pii.view.reader" TO ROLE  "RAW.STITCH_TYPEFORM.no_pii.reader.role" ;
GRANT ROLE "RAW.STITCH_TYPEFORM.ANSWERS.no_pii.table.reader" TO ROLE "RAW.STITCH_TYPEFORM.no_pii.reader.role" ;
GRANT ROLE "RAW.STITCH_TYPEFORM.QUESTIONS.no_pii.table.reader" TO ROLE "RAW.STITCH_TYPEFORM.no_pii.reader.role" ;

-- ADD GROUP ROLES TO THE DBT ROLE
GRANT ROLE "RAW.STITCH_TYPEFORM.no_pii.reader.role"    TO ROLE DBT_DEV;
GRANT ROLE "RAW.STITCH_TYPEFORM.no_pii.reader.role"    TO ROLE DBT_PROD;


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