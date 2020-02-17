CREATE OR REPLACE VIEW "WEB_DATA"."SHOPIFY"."VW_ORDERS__LINE_ITEMS" COPY GRANTS AS (

SELECT

    o."ID" AS "ORDER_ID",
    f1.value:origin_location:name::string as "ORIGIN_LOCATION",
	f1.value:admin_graphql_api_id::string as "LINE_ITEMS__ADMIN_GRAPHQL_API_ID",
    f1.value:fulfillment_status::string as "LINE_ITEMS__FULFILLMENT_STATUS",
    f1.value:gift_card::boolean as "LINE_ITEMS__GIFT_CARD",
    f1.value:id::number as "LINE_ITEMS__ID",
    f1.value:name::string as "LINE_ITEMS__NAME",
    f1.value:price::float as "LINE_ITEMS__PRICE",
    f1.value:product_exists::boolean as "LINE_ITEMS__PRODUCT_EXISTS",
    f1.value:product_id::number as "LINE_ITEMS__PRODUCT_ID",
    f1.value:quantity::number as "LINE_ITEMS__QUANTITY",
    f1.value:sku::string as "LINE_ITEMS__SKU",
    f1.value:taxable::boolean as "LINE_ITEMS__TAXABLE",
    f1.value:title::string as "LINE_ITEMS__TITLE",
    f1.value:total_discount::float as "LINE_ITEMS__TOTAL_DISCOUNT",
    f1.value:variant_id::number as "LINE_ITEMS__VARIANT_ID",
   	f1.value:variant_title::string as "LINE_ITEMS__VARIANT_TITLE",
    f1.value:vendor::string as "LINE_ITEMS__VENDOR"

FROM "RAW"."STITCH_SHOPIFY_NEW"."ORDERS" o
LEFT JOIN "DATAENG_UTILS"."MAPPINGS"."PII_MAPPINGS" PII ON PII.ROLE = CURRENT_ROLE()
, lateral flatten(input => parse_json(LINE_ITEMS)) f1

);