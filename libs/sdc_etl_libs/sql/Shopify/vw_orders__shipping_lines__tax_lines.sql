CREATE OR REPLACE VIEW "WEB_DATA"."SHOPIFY"."VW_ORDERS__SHIPPING_LINES__TAX_LINES" COPY GRANTS AS (

SELECT

    o."ID" AS "ORDER_ID",
	f1.value:id::number as "SHIPPING_LINES__ID",
	f2.value:price::float as "SHIPPING_ITEMS__TAX_LINES__PRICE",
	f2.value:rate::float as "SHIPPING_ITEMS__TAX_LINES__RATE",
	f2.value:title::string as "SHIPPING_ITEMS__TAX_LINES__TITLE"

FROM "RAW"."STITCH_SHOPIFY_NEW"."ORDERS" o
LEFT JOIN "DATAENG_UTILS"."MAPPINGS"."PII_MAPPINGS" PII ON PII.ROLE = CURRENT_ROLE()
, lateral flatten(input => parse_json(shipping_lines)) f1
, lateral flatten(input => parse_json(f1.value:tax_lines)) f2

);