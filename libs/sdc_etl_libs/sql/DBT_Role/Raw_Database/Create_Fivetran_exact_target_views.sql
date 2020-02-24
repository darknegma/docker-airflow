-- UPDATE PLAN
-- 3 TABLES ARE BEING USED BY DBT
-- 2 NO_PII TABLES - CREATE THE INDIVIDUAL READ ROLES FOR THESE
-- 1 PII TABLE-- 1 VIEWS NEEDED
-- ADD THE 3 read ROLES TO THE NO_PII GROUP ROLE
-- ADD THE NO_PII GROUP ROLE TO BOTH DBT ROLES

--RAW.FIVETRAN_EXACT_TARGET_DIM.LIST_SUBSCRIBER
--RAW.FIVETRAN_EXACT_TARGET_DIM.SUBSCRIBER
--RAW.FIVETRAN_EXACT_TARGET_EVENT.EVENT

USE ROLE AIRFLOW_SERVICE_ROLE;
USE DATABASE RAW;
USE SCHEMA FIVETRAN_EXACT_TARGET_DIM;

CREATE OR REPLACE VIEW VW_SUBSCRIBER  COPY GRANTS  AS
(
  SELECT
        ID	,
        CASE WHEN PII_ACCESS = TRUE THEN EMAIL_ADDRESS ELSE sha2(EMAIL_ADDRESS,512) END AS EMAIL_ADDRESS,
        EMAIL_TYPE_PREFERENCE	,
        STATUS	,
        CREATED_DATE,
        UNSUBSCRIBED_DATE	,
        _FIVETRAN_DELETED	,
        _FIVETRAN_SYNCED
        SUBSCRIBER_KEY

  FROM SUBSCRIBER
  LEFT JOIN DATAENG_UTILS.MAPPINGS.PII_MAPPINGS PII ON PII.ROLE = CURRENT_ROLE()
);


USE ROLE SECURITYADMIN;

-- CREATE THE GROUP ROLES
CREATE ROLE "RAW.FIVETRAN_EXACT_TARGET_DIM.no_pii.reader.role" ;

-- ADD PERMISIONS
GRANT USAGE ON DATABASE RAW TO ROLE "RAW.FIVETRAN_EXACT_TARGET_DIM.no_pii.reader.role" ;
GRANT USAGE ON SCHEMA RAW.FIVETRAN_EXACT_TARGET_DIM TO ROLE "RAW.FIVETRAN_EXACT_TARGET_DIM.no_pii.reader.role" ;

-- CREATE THE INDIVIDUAL TABLE ROLES AND ONE VIEW ROLE
CREATE ROLE "RAW.FIVETRAN_EXACT_TARGET_DIM.LIST_SUBSCRIBER.no_pii.table.reader";
CREATE ROLE "RAW.FIVETRAN_EXACT_TARGET_DIM.SUBSCRIBER.no_pii.view.reader";

-- ADD THE INVIDUAL ROLES TO THIER TABLES/VIEW
GRANT SELECT ON TABLE "RAW"."FIVETRAN_EXACT_TARGET_DIM"."LIST_SUBSCRIBER" TO ROLE "RAW.FIVETRAN_EXACT_TARGET_DIM.LIST_SUBSCRIBER.no_pii.table.reader";
GRANT SELECT ON VIEW  "RAW"."FIVETRAN_EXACT_TARGET_DIM"."VW_SUBSCRIBER" TO ROLE "RAW.FIVETRAN_EXACT_TARGET_DIM.SUBSCRIBER.no_pii.view.reader";

-- ADD THE INVIDUAL ROLES TO ITS GrOUP ROLES
GRANT ROLE  "RAW.FIVETRAN_EXACT_TARGET_DIM.LIST_SUBSCRIBER.no_pii.table.reader" TO ROLE "RAW.FIVETRAN_EXACT_TARGET_DIM.no_pii.reader.role" ;
GRANT ROLE  "RAW.FIVETRAN_EXACT_TARGET_DIM.SUBSCRIBER.no_pii.view.reader" TO ROLE "RAW.FIVETRAN_EXACT_TARGET_DIM.no_pii.reader.role" ;

-- ADD GROUP ROLES TO THE DBT ROLE
GRANT ROLE "RAW.FIVETRAN_EXACT_TARGET_DIM.no_pii.reader.role" TO ROLE DBT_DEV;
GRANT ROLE "RAW.FIVETRAN_EXACT_TARGET_DIM.no_pii.reader.role"  TO ROLE DBT_PROD;


-------############################################################

USE SCHEMA FIVETRAN_EXACT_TARGET_EVENT;

-- CREATE THE GROUP ROLES
CREATE ROLE "RAW.FIVETRAN_EXACT_TARGET_EVENT.no_pii.reader.role" ;

-- ADD PERMISIONS
GRANT USAGE ON DATABASE RAW TO ROLE "RAW.FIVETRAN_EXACT_TARGET_EVENT.no_pii.reader.role" ;
GRANT USAGE ON SCHEMA RAW.FIVETRAN_EXACT_TARGET_EVENT TO ROLE "RAW.FIVETRAN_EXACT_TARGET_EVENT.no_pii.reader.role" ;

-- CREATE THE INDIVIDUAL TABLE ROLES AND ONE VIEW ROLE
CREATE ROLE "RAW.FIVETRAN_EXACT_TARGET_EVENT.EVENT.no_pii.table.reader";

-- ADD THE INVIDUAL ROLES TO THIER TABLES/VIEW
GRANT SELECT ON TABLE "RAW"."FIVETRAN_EXACT_TARGET_EVENT"."EVENT" TO ROLE "RAW.FIVETRAN_EXACT_TARGET_EVENT.EVENT.no_pii.table.reader";

-- ADD THE INVIDUAL ROLES TO ITS GrOUP ROLES
GRANT ROLE  "RAW.FIVETRAN_EXACT_TARGET_EVENT.EVENT.no_pii.table.reader" TO ROLE "RAW.FIVETRAN_EXACT_TARGET_EVENT.no_pii.reader.role" ;

-- ADD GROUP ROLES TO THE DBT ROLE
GRANT ROLE "RAW.FIVETRAN_EXACT_TARGET_EVENT.no_pii.reader.role"  TO ROLE DBT_DEV;
GRANT ROLE "RAW.FIVETRAN_EXACT_TARGET_EVENT.no_pii.reader.role"  TO ROLE DBT_PROD;



-------############################################################

-- TEST AND JUNK STUFF BELOW ----------------------------

------###############################################################

USE WAREHOUSE TRANSFORMING_WAREHOUSE;
USE DATABASE RAW;
USE SCHEMA FIVETRAN_EXACT_TARGET_DIM;

show grants on schema FIVETRAN_EXACT_TARGET_DIM;
FIVETRAN_ROLE
TRANSFORMER


show grants to ROLE FIVETRAN_ROLE ;
show grants ON ROLE FIVETRAN_ROLE;

SHOW GRANTS ON TABLE
"RAW"."FIVETRAN_EXACT_TARGET_DIM"."LIST_SUBSCRIBER"
-- TRANSFORMER HAS SELECTS DIRECTLY ON THE TABLES