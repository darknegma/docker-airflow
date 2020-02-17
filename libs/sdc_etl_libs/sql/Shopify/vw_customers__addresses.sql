CREATE OR REPLACE VIEW "WEB_DATA"."SHOPIFY"."VW_CUSTOMERS__ADDRESSES" COPY GRANTS AS (

SELECT

    ID AS CUSTOMER_ID,
    CASE WHEN PII_ACCESS = TRUE THEN f1.value:address1::string else sha2(f1.value:address1::string, 512) END as "ADDRESSES__ADDRESS1",
    CASE WHEN PII_ACCESS = TRUE THEN f1.value:address2::string else sha2(f1.value:address2::string, 512) END AS "ADDRESSES__ADDRESS2",
    f1.value:city::string AS "ADDRESSES__CITY",
    f1.value:country_code::string AS "ADDRESSES__COUNTRY_CODE",
    f1.value:country::string AS "ADDRESSES__COUNTRY",
    F1.value:default::boolean AS ADDRESSES__IS_DEFAULT,
    f1.value:id::number AS ADDRESSES__ADDRESS_ID,
    CASE WHEN PII_ACCESS = TRUE THEN f1.value:name::string else sha2(f1.value:name::string, 512) END AS "ADDRESSES__CUSTOMER_NAME",
    f1.value:province::string AS "ADDRESSES__PROVINCE",
    f1.value:province_code::string AS "ADDRESSES__PROVINCE_CODE",
    f1.value:zip::string AS "ADDRESSES__ZIP",
    CASE WHEN PII_ACCESS = TRUE THEN f1.value:phone::string else sha2(f1.value:phone::string, 512) END AS "ADDRESSES__PHONE"

FROM "RAW"."STITCH_SHOPIFY_NEW"."CUSTOMERS"
LEFT JOIN "DATAENG_UTILS"."MAPPINGS"."PII_MAPPINGS" PII ON PII.ROLE = CURRENT_ROLE()
, LATERAL FLATTEN(INPUT => PARSE_JSON(ADDRESSES)) F1

);