CREATE OR REPLACE VIEW "WEB_DATA"."SHOPIFY"."VW_ORDERS__SHIPPING_LINES" COPY GRANTS AS (

SELECT

    o."ID" AS "ORDER_ID",
	f1.value:code::string as "SHIPPING_LINES__CODE",
	f1.value:discounted_price::float as "SHIPPING_LINES__DISCOUNTED_PRICE",
	f1.value:id::number as "SHIPPING_LINES__ID",
	f1.value:price::float as "SHIPPING_LINES__PRICE"


FROM "RAW"."STITCH_SHOPIFY_NEW"."ORDERS" o
LEFT JOIN "DATAENG_UTILS"."MAPPINGS"."PII_MAPPINGS" PII ON PII.ROLE = CURRENT_ROLE()
, lateral flatten(input => parse_json(shipping_lines)) f1

);