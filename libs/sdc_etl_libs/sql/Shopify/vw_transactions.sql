CREATE OR REPLACE VIEW "WEB_DATA"."SHOPIFY"."VW_TRANSACTIONS" COPY GRANTS AS (

SELECT

    t.amount,
	t.created_at,
	t.currency,
	t.gateway,
	t.id as transaction_id,
	t.kind,
	t.message,
	t.order_id,
	o.name as order_number,
	t.parent_id,
	CASE WHEN PII_ACCESS = TRUE THEN t.receipt ELSE '{}' END AS "RECEIPT",
	t.source_name,
	t.status,
	t.test,
	CASE WHEN PII_ACCESS = TRUE THEN t.payment_details:avs_result_code::string  ELSE SHA2(t.payment_details:avs_result_code::string, 512) END AS "AVS_RESULT_CODE",
	CASE WHEN PII_ACCESS = TRUE THEN t.payment_details:credit_card_company::string  ELSE SHA2(t.payment_details:credit_card_company::string, 512) END AS "CREDIT_CARD_COMPANY",
	CASE WHEN PII_ACCESS = TRUE THEN t.payment_details:credit_card_bin::string  ELSE SHA2(t.payment_details:credit_card_bin::string, 512) END AS "CREDIT_CARD_BIN",
	CASE WHEN PII_ACCESS = TRUE THEN t.payment_details:cvv_result_code::string  ELSE SHA2(t.payment_details:cvv_result_code::string, 512) END AS "CVV_RESULT_CODE",
	t.authorization,
	t.receipt:amount/100::float as "RECEIPT__AMOUNT",
	t.receipt:amount_refunded/100::float as "RECEIPT__AMOUNT_REFUNDED",
	t.receipt:application::string as "RECEIPT__APPLICATION",
	t.receipt:captured::boolean as "RECEIPT__CAPTURED",
	t.receipt:currency::string as "RECEIPT__CURRENCY",
	t.receipt:id::string as "RECEIPT__CHARGE_ID"

FROM "RAW"."STITCH_SHOPIFY_NEW"."TRANSACTIONS" t
left join "RAW"."STITCH_SHOPIFY_NEW"."ORDERS" o on t.order_id = o.id
LEFT JOIN "DATAENG_UTILS"."MAPPINGS"."PII_MAPPINGS" PII ON PII.ROLE = CURRENT_ROLE()

);