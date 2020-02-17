CREATE OR REPLACE TABLE "MEDICAL_DATA"."TRUEVAULT"."FACT_DENTAL_HEALTH_QUESTIONNAIRE_PII" COPY GRANTS AS

    SELECT

        cc.case_number AS CASE_NUMBER,
        cpaa.date_created AS DATE_CREATED,
        upper(pua.region) AS CUSTOMER_STATE,
        q.*

    FROM "MEDICAL_DATA"."TRUEVAULT"."DENTAL_HEALTH_QUESTIONNAIRE_PII" q
    INNER JOIN "RAW"."FIVETRAN_TESSERACT_PUBLIC"."CASES_PHOTO_ASSESSMENT_ASSET_DATASTORE" cpaa ON (q.external_key = cpaa.external_key)
    INNER JOIN "RAW"."FIVETRAN_TESSERACT_PUBLIC"."CASES_PHOTO_ASSESSMENT" cpa ON (cpaa.photo_assessment_id = cpa.id)
    INNER JOIN "RAW"."FIVETRAN_TESSERACT_PUBLIC"."CASES_CASE" cc ON (cpa.case_id = cc.id)
    INNER JOIN "RAW"."FIVETRAN_TESSERACT_PUBLIC"."PROFILES_USER_PATIENT" pup ON (cc.patient_id = pup.patient_id)
    INNER JOIN
    (
        SELECT DISTINCT o.USER_ID AS USER_ID
        FROM "RAW"."FIVETRAN_TESSERACT_PUBLIC"."COMMERCE_ORDER" o
        INNER JOIN "RAW"."FIVETRAN_TESSERACT_PUBLIC"."COMMERCE_ORDER_ITEM" oi
            ON o.id = oi.order_id
        INNER JOIN "RAW"."FIVETRAN_TESSERACT_PUBLIC"."COMMERCE_PRODUCT" p ON p.id = oi.product_id
        INNER JOIN "RAW"."FIVETRAN_TESSERACT_PUBLIC"."COMMERCE_PRODUCT_CATEGORY" pc
            ON pc.id = p.product_category_id
        LEFT JOIN "RAW"."FIVETRAN_TESSERACT_PUBLIC"."COMMERCE_DISCOUNT" disc
            ON o.discount_id = disc.id
        WHERE pc.name = 'evalkit'
          AND o.date_removed IS NULL
          AND o.date_order IS NOT NULL
    ) KITO ON KITO.USER_ID = pup.USER_ID
    LEFT JOIN
    (
        SELECT
            PU.id AS USER_ID,
            Y.VALUE AS ADDRESS_ID
        FROM "RAW"."FIVETRAN_TESSERACT_PUBLIC"."PROFILES_USER" pu
        LEFT JOIN
        (
            SELECT
                pupf.value,
                pupf.user_id
            FROM "RAW"."FIVETRAN_TESSERACT_PUBLIC"."PROFILES_USER_PREFERENCE" pupf
            WHERE pupf.key ='shipping_address'
        ) y ON pu.id = y.user_id
        WHERE y.value IS NOT NULL
    ) MAIN ON MAIN.user_id = pup.user_id

    LEFT JOIN "RAW"."FIVETRAN_TESSERACT_PUBLIC"."PROFILES_USER_ADDRESS" PUA ON PUA.UUID::text = MAIN.ADDRESS_ID::text

;
