-- UPDATE PLAN
-- 1 TABLES ARE BEING USED BY DBT
-- 0 NO_PII TABLES - CREATE THE INDIVIDUAL READ ROLES FOR THESE
-- 1 PII TABLE-- 1 VIEWS NEEDED
-- ADD THE  read ROLES TO THE NO_PII GROUP ROLE
-- ADD THE NO_PII GROUP ROLE TO BOTH DBT ROLES

--RAW.STITCH_SHOPIFY_NEW.ORDERS

USE ROLE AIRFLOW_SERVICE_ROLE;
USE DATABASE RAW;
USE SCHEMA STITCH_SHOPIFY_NEW;

CREATE OR REPLACE VIEW VW_ORDERS  COPY GRANTS  AS
(

-- THIS ONE HAS A TENDENCY TO HAVE WIERD CHARACTERS IN THE JSON FIELD THAT WILL CAUSE AN ERROR ON THE REPLACE
-- SO WE DOING A STEP BY STER APPROACH - CHECKING FOR VAlID JSON BEFORE CONVERTING
-- VALUES TO CHECK
-- 1. name
-- 2. last_name
-- 3. address1

-- name
  WITH REPLACE_BILLING_ADDRESS_NAME AS
  (

   SELECT
         _SDC_SEQUENCE,
         PII_ACCESS,
         CASE WHEN PII_ACCESS = TRUE THEN BILLING_ADDRESS
              ELSE
                CASE WHEN CHECK_JSON(
                                        REPLACE(
                                                    BILLING_ADDRESS, BILLING_ADDRESS:name, sha2(BILLING_ADDRESS:name,512)
                                               )
                                    ) IS NULL THEN
                                              PARSE_JSON(
                                                         REPLACE(
                                                                   BILLING_ADDRESS, BILLING_ADDRESS:name, sha2(BILLING_ADDRESS:name,512)
                                                                )
                                                        )
                                             ELSE
                                               PARSE_JSON('{"ERROR":"ERROR PARSING JSON"}')
        END
        END AS BILLING_ADDRESS

  FROM ORDERS
  LEFT JOIN DATAENG_UTILS.MAPPINGS.PII_MAPPINGS PII ON PII.ROLE = CURRENT_ROLE()

)    ,

-- last_name
REPLACE_BILLING_ADDRESS_LAST_NAME as
  (
      SELECT
         _SDC_SEQUENCE,
         PII_ACCESS,
         CASE WHEN PII_ACCESS = TRUE THEN BILLING_ADDRESS
              ELSE
                CASE WHEN CHECK_JSON(
                                        REPLACE(
                                                    BILLING_ADDRESS, BILLING_ADDRESS:last_name, sha2(BILLING_ADDRESS:last_name,512)
                                               )
                                    ) IS NULL THEN
                                              PARSE_JSON(
                                                         REPLACE(
                                                                   BILLING_ADDRESS, BILLING_ADDRESS:last_name, sha2(BILLING_ADDRESS:last_name,512)
                                                                )
                                                        )
                                             ELSE
                                               PARSE_JSON('{"ERROR":"ERROR PARSING JSON"}')
        END
        END AS BILLING_ADDRESS

  FROM REPLACE_BILLING_ADDRESS_NAME

  ),

-- address1
  REPLACE_BILLING_ADDRESS_ADDRESS1 as
  (
      SELECT
         _SDC_SEQUENCE,
         PII_ACCESS,
         CASE WHEN PII_ACCESS = TRUE THEN BILLING_ADDRESS
              ELSE
                CASE WHEN CHECK_JSON(
                                        REPLACE(
                                                    BILLING_ADDRESS, BILLING_ADDRESS:address1, sha2(BILLING_ADDRESS:address1,512)
                                               )
                                    ) IS NULL THEN
                                              PARSE_JSON(
                                                         REPLACE(
                                                                   BILLING_ADDRESS, BILLING_ADDRESS:address1, sha2(BILLING_ADDRESS:address1,512)
                                                                )
                                                        )
                                             ELSE
                                               PARSE_JSON('{"ERROR":"ERROR PARSING JSON"}')
        END
        END AS BILLING_ADDRESS

  FROM REPLACE_BILLING_ADDRESS_LAST_NAME

  )

  SELECT
        ADMIN_GRAPHQL_API_ID	,
        APP_ID	,
        REP.BILLING_ADDRESS,
        BROWSER_IP	,
        BUYER_ACCEPTS_MARKETING	,
        CANCELLED_AT	,
        CANCEL_REASON	,
        CART_TOKEN	,
        CHECKOUT_ID ,
        CHECKOUT_TOKEN	,
        CLIENT_DETAILS	,
        CLOSED_AT	,
        CONFIRMED	,
        CONTACT_EMAIL	,
        CREATED_AT	,
        CURRENCY	,
        CUSTOMER	,
        CUSTOMER_LOCALE	,
        DISCOUNT_APPLICATIONS	,
        DISCOUNT_CODES	,
        CASE WHEN PII_ACCESS = TRUE THEN EMAIL ELSE sha2(EMAIL,512) END AS  EMAIL,
        FINANCIAL_STATUS	,
        FULFILLMENTS	,
        FULFILLMENT_STATUS	,
        GATEWAY	,
        ID	,
        LANDING_SITE	,
        LANDING_SITE_REF	,
        LINE_ITEMS	,
        NAME	,
        NOTE_ATTRIBUTES	,
        NUMBER	,
        ORDER_NUMBER	,
        ORDER_STATUS_URL	,
        PAYMENT_DETAILS	,
        PAYMENT_GATEWAY_NAMES	,
        PRESENTMENT_CURRENCY	,
        PROCESSED_AT	,
        PROCESSING_METHOD	,
        REFERRING_SITE	,
        REFUNDS	 ,
        SHIPPING_ADDRESS	,
        SHIPPING_LINES	,
        SOURCE_NAME	,
        SUBTOTAL_PRICE	,
        SUBTOTAL_PRICE_SET	,
        TAGS	,
        TAXES_INCLUDED	,
        TAX_LINES	,
        TEST	,
        TOKEN	,
        TOTAL_DISCOUNTS	,
        TOTAL_DISCOUNTS_SET	,
        TOTAL_LINE_ITEMS_PRICE	,
        TOTAL_LINE_ITEMS_PRICE_SET	,
        TOTAL_PRICE	,
        TOTAL_PRICE_SET	,
        TOTAL_PRICE_USD	,
        TOTAL_SHIPPING_PRICE_SET	,
        TOTAL_TAX	,
        TOTAL_TAX_SET	,
        TOTAL_TIP_RECEIVED	,
        TOTAL_WEIGHT	,
        UPDATED_AT	,
        _SDC_BATCHED_AT	,
        _SDC_EXTRACTED_AT	,
        _SDC_RECEIVED_AT	,
        ORDERS._SDC_SEQUENCE	,
        _SDC_TABLE_VERSION	,
        SOURCE_IDENTIFIER	,
        NOTE	,
        PHONE	,
        LOCATION_ID	,
        DEVICE_ID	,
        USER_ID	,
        REFERENCE
  FROM  ORDERS
  INNER JOIN REPLACE_BILLING_ADDRESS_ADDRESS1 REP ON ORDERS._SDC_SEQUENCE = REP._SDC_SEQUENCE

);

USE ROLE SECURITYADMIN;

-- CREATE THE GROUP ROLES
CREATE ROLE "RAW.STITCH_SHOPIFY_NEW.no_pii.reader.role" ;

-- ADD PERMISIONS
GRANT USAGE ON DATABASE RAW TO ROLE "RAW.STITCH_SHOPIFY_NEW.no_pii.reader.role" ;
GRANT USAGE ON SCHEMA RAW.STITCH_SHOPIFY_NEW TO ROLE "RAW.STITCH_SHOPIFY_NEW.no_pii.reader.role" ;

-- CREATE THE INDIVIDUAL TABLE ROLES AND ONE VIEW ROLE
CREATE ROLE "RAW.STITCH_SHOPIFY_NEW.ORDERS.no_pii.view.reader";

-- ADD THE INVIDUAL ROLES TO THIER TABLES/VIEW
GRANT SELECT ON VIEW "RAW"."STITCH_SHOPIFY_NEW"."VW_ORDERS" TO ROLE "RAW.STITCH_SHOPIFY_NEW.ORDERS.no_pii.view.reader";

-- ADD THE INVIDUAL ROLES TO ITS GrOUP ROLES
GRANT ROLE "RAW.STITCH_SHOPIFY_NEW.ORDERS.no_pii.view.reader" TO ROLE "RAW.STITCH_SHOPIFY_NEW.no_pii.reader.role" ;

-- ADD GROUP ROLES TO THE DBT ROLE
GRANT ROLE "RAW.STITCH_SHOPIFY_NEW.no_pii.reader.role"    TO ROLE DBT_DEV;
GRANT ROLE "RAW.STITCH_SHOPIFY_NEW.no_pii.reader.role"    TO ROLE DBT_PROD;


-------############################################################


-------############################################################

-- TEST AND JUNK STUFF BELOW ----------------------------

------###############################################################

USE WAREHOUSE TRANSFORMING_WAREHOUSE;
USE DATABASE RAW;
USE SCHEMA STITCH_PODIUM;

show grants on schema STITCH_PODIUM;
STITCH_ROLE
TRANSFORMER


show grants to ROLE FIVETRAN_ROLE ;
show grants ON ROLE FIVETRAN_ROLE;

SHOW GRANTS ON TABLE
"RAW"."STITCH_NETSUITE"."NETSUITE_TRANSACTION"
-- TRANSFORMER HAS OWNE