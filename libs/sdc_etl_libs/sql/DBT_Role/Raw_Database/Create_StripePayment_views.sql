-- UPDATE PLAN
-- 3 VIEWS ARE BEING USED BY DBT
-- ADD THE ALREADY SETUP NON PII GROUP ROLE TO DBT

--RAW.STRIPE.PAYMENT_METHOD_DETAILS_AUS_NON_PII_SECURE_VIEW
--RAW.STRIPE.PAYMENT_METHOD_DETAILS_CAN_NON_PII_SECURE_VIEW
--RAW.STRIPE.PAYMENT_METHOD_DETAILS_USA_NON_PII_SECURE_VIEW

USE ROLE SECURITYADMIN;

-- ADD GROUP ROLES TO THE DBT ROLE
GRANT ROLE "RAW.STRIPE.non_pii.reader.role"    TO ROLE DBT_DEV;
GRANT ROLE "RAW.STRIPE.non_pii.reader.role"    TO ROLE DBT_PROD;


-------############################################################


-------############################################################

-- TEST AND JUNK STUFF BELOW ----------------------------

------###############################################################

USE WAREHOUSE TRANSFORMING_WAREHOUSE;
USE DATABASE RAW;
USE SCHEMA STRIPE;

show grants on schema STRIPE;
RAW.STRIPE.non_pii.reader.role
TRANSFORMING_WAREHOUSE.RAW.STRIPE.pii.reader.role


show grants to role "RAW.STRIPE.non_pii.reader.role" ;
-- has select on all 3 VIEWS here
show grants ON role  "RAW.STRIPE.non_pii.reader.role" ;
-- transformer has hsage

show grants to role "TRANSFORMING_WAREHOUSE.RAW.STRIPE.pii.reader.role" ;
-- has all or most tables
show grants ON role  "TRANSFORMING_WAREHOUSE.RAW.STRIPE.pii.reader.role" ;
--transforming_warehouse.analytics.finance.pii.role
--SECURITYADMIN

show grants ON role  "transforming_warehouse.analytics.finance.pii.role" ;
TRANSFORMING.PAULHOWELL_SANDBOX_DB.pii.user.role


SHOW GRANTS of "TRANSFORMING.PAULHOWELL_SANDBOX_DB.pii.user.role" ;
PAULHOWELL
MATTHEWGARCIA

SHOW GRANTS ON VIEW "RAW"."STRIPE"."PAYMENT_METHOD_DETAILS_USA_NON_PII_SECURE_VIEW"
SHOW GRANTS ON VIEW "RAW"."STRIPE"."PAYMENT_METHOD_DETAILS_CAN_NON_PII_SECURE_VIEW"
SHOW GRANTS ON VIEW "RAW"."STRIPE"."PAYMENT_METHOD_DETAILS_AUS_NON_PII_SECURE_VIEW"


SHOW GRANTS ON TABLE "RAW"."STRIPE"."PAYMENT_METHOD_DETAILS_USA_PII"
