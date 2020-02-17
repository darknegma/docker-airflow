CREATE OR REPLACE VIEW "WEB_DATA"."SHOPIFY"."VW_CUSTOMERS" COPY GRANTS AS (

SELECT

    "ID",
    "CREATED_AT",
    CASE WHEN PII_ACCESS = TRUE THEN "EMAIL" ELSE sha2("EMAIL", 512) END AS "EMAIL",
    CASE WHEN PII_ACCESS = TRUE THEN "FIRST_NAME" ELSE sha2("FIRST_NAME", 512) END AS "FIRST_NAME",
    CASE WHEN PII_ACCESS = TRUE THEN "LAST_NAME" ELSE sha2("LAST_NAME", 512) END AS "LAST_NAME",
    "LAST_ORDER_ID",
    "LAST_ORDER_NAME",
    CASE WHEN PII_ACCESS = TRUE THEN default_address:address1::string else sha2(default_address:address1::string, 512) END AS "ADDRESS1",
    CASE WHEN PII_ACCESS = TRUE THEN default_address:address2::string else sha2(default_address:address2::string, 512) END AS "ADDRESS2",
    default_address:city::string AS "CITY",
    default_address:country AS "COUNTRY",
    default_address:default::BOOLEAN AS "IS_DEFAULT",
    default_address:id::NUMBER AS "ADDRESS_ID",
    CASE WHEN PII_ACCESS = TRUE THEN default_address:name::string else sha2(default_address:name::string, 512) END AS "CUSTOMER_NAME",
    default_address:province::string AS "PROVINCE",
    default_address:province_code::string AS "PROVINCE_CODE",
    default_address:zip::string AS "ZIP",
    "NOTE",
    "ORDERS_COUNT",
    CASE WHEN PII_ACCESS = TRUE THEN "PHONE" else "PHONE" END AS "PHONE",
    "TAGS",
    "TOTAL_SPENT"

FROM "RAW"."STITCH_SHOPIFY_NEW"."CUSTOMERS"
LEFT JOIN "DATAENG_UTILS"."MAPPINGS"."PII_MAPPINGS" PII ON PII.ROLE = CURRENT_ROLE()

);