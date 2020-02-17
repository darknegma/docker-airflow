
WITH L1 AS (
  SELECT
    LL.ID AS CUSTID, 
    '' AS STREET, 
    '' AS SECONDARY, 
    '' AS CITY, 
    '' AS STATE,
    '' AS POSTAL,
    LL.PHONE AS PHONE, 
    LL.EMAIL AS EMAIL, 
    LL.DATE_LEAD AS TRANSACTION_DATE, 
    'LEAD' AS TRANSACTION_TYPE, 
    '0.00' AS TRANSACTION_VALUE, 
    CASE WHEN LL.KIND = 'INS' THEN
      'Insurance'
    ELSE
      'Direct'
    END AS REFERRAL, 
    'Online' AS LOCATION, 
    '' AS PAYMENT, 
    '' AS FINANCED,
    '0.00' AS DISCOUNT_AMOUNT,
    '' AS DISCOUNT_CODE,
    LASTUTM.UTM_SOURCE AS UTM_SOURCE
FROM raw.fivetran_tesseract_public.LEADS_LEAD LL
LEFT JOIN (
          SELECT l.email, firstlc.utm_source AS UTM_SOURCE
          FROM raw.fivetran_tesseract_public.leads_lead l
           INNER JOIN
           (
               SELECT ll.email, MAX(id) AS maxRec
               FROM raw.fivetran_tesseract_public.leads_lead ll
               GROUP BY ll.email
           ) x on x.maxrec = l.id
           LEFT JOIN raw.fivetran_tesseract_public.leads_campaign firstlc ON l.campaign_id = firstlc.id
  ) LASTUTM ON LASTUTM.email = LL.EMAIL
),
y0 AS (
    SELECT ID, EMAIL, DATE_LEAD, KIND
    FROM raw.fivetran_tesseract_public.leads_lead 
    WHERE date_removed IS NULL 
),
minRec as (
    SELECT MIN(ID) as MinRec
    FROM raw.fivetran_tesseract_public.leads_lead
    WHERE date_removed IS NULL 
    GROUP BY email
),
LL as (
   SELECT ID, EMAIL, DATE_LEAD, KIND
    FROM y0 INNER JOIN minRec ON minRec.MinRec = y0.id
),
y1 AS (
SELECT ID, USER_ID, STREET1, STREET2, LOCALITY, REGION, POSTAL_CODE
      FROM raw.fivetran_tesseract_public.PROFILES_USER_ADDRESS 
     WHERE KIND = 'SHIPPING'
),
minRec2 AS (
SELECT MIN(ID) AS MinRec
       FROM raw.fivetran_tesseract_public.PROFILES_USER_ADDRESS
      WHERE KIND = 'SHIPPING'
     GROUP BY USER_ID
),
PUA AS (
        SELECT y1.ID, y1.USER_ID, y1.STREET1, y1.STREET2, y1.LOCALITY, y1.REGION, y1.POSTAL_CODE
        FROM y1 INNER JOIN minRec2 ON minRec2.MinRec = y1.id
),
L2 AS (
        SELECT
    MIN(LL.ID) AS CUSTID, 
    PUA.STREET1 AS STREET, 
    PUA.STREET2 AS SECONDARY, 
    PUA.LOCALITY AS CITY,
    PUA.REGION AS STATE, 
    PUA.POSTAL_CODE AS POSTAL,
    PH.PHONE AS PHONE, 
    LL.EMAIL AS EMAIL, 
    ALIGNERO.DATE_ORDER AS TRANSACTION_DATE, 
    'ALIGNER' AS TRANSACTION_TYPE, 
    ALIGNERO.ALIGNER_SALE_PRICE AS TRANSACTION_VALUE, 
    CASE WHEN LL.KIND = 'INS' THEN
       'Insurance'
    ELSE
       'Direct'
    END AS REFERRAL, 
    'Online' AS LOCATION, 
    CASE WHEN ALIGNERO.ALIGNER_DISCOUNT_APPLIED = 1 THEN
       'DISCOUNT'
    ELSE
       'FULL'
    END AS PAYMENT, 
    CASE WHEN ALIGNERO.ALIGNER_FINANCED = 1 THEN
     'YES'
    ELSE
     'NO' 
    END AS FINANCED,
    ALIGNERO.ALIGNER_DISCOUNT_AMOUNT AS DISCOUNT_AMOUNT,
    ALIGNERO.ALIGNER_DISCOUNT_CODE AS DISCOUNT_CODE,
    LASTUTM.UTM_SOURCE AS UTM_SOURCE
FROM 
LL
LEFT JOIN (
           SELECT l.email, firstlc.utm_source AS UTM_SOURCE
           FROM raw.fivetran_tesseract_public.leads_lead l
           INNER JOIN
           (
               SELECT ll.email, MAX(id) AS maxRec
               FROM raw.fivetran_tesseract_public.leads_lead ll
               GROUP BY ll.email
           ) x on x.maxrec = l.id
           LEFT JOIN raw.fivetran_tesseract_public.leads_campaign firstlc ON l.campaign_id = firstlc.id
) LASTUTM ON LASTUTM.email = LL.EMAIL
INNER JOIN 
   (SELECT PU.id AS USER_ID, PU.username AS email
         FROM raw.fivetran_tesseract_public.profiles_user PU
      ) MAIN 
   ON MAIN.EMAIL = LL.EMAIL
INNER JOIN PUA ON (PUA.user_id = MAIN.user_id)
INNER JOIN raw.fivetran_tesseract_public.profiles_user_phone ph ON PH.user_id = MAIN.USER_ID
INNER JOIN 
 (
    -- INITIAL ALIGNER ORDERS
    SELECT ID, USER_ID, DATE_ORDER, ALIGNER_SALE_PRICE, ALIGNER_DISCOUNT_APPLIED,
          ALIGNER_FULL_PAYMENT, ALIGNER_FINANCED, ALIGNER_DISCOUNT_AMOUNT, ALIGNER_DISCOUNT_CODE
     FROM 
          (SELECT o.id, o.user_id, o.date_order, to_char( o.total_amt / 100.0, 'FM999999990.00' ) as ALIGNER_SALE_PRICE, 
             CASE WHEN o.discount_amt = 0 THEN
                0
             ELSE
                1
             END AS ALIGNER_DISCOUNT_APPLIED, 
             CASE WHEN oi.payment_plan = 'FULLPAY' THEN
                1
             ELSE
                0
             END AS ALIGNER_FULL_PAYMENT, 
             CASE WHEN oi.payment_plan = 'FULLPAY' THEN
                0
             ELSE
                1
             END AS ALIGNER_FINANCED,
             to_char( o.discount_amt / 1.0, 'FM999999990.00' ) AS ALIGNER_DISCOUNT_AMOUNT,
             CASE WHEN disc.coupon_code IS NULL THEN
               ''
             ELSE
               disc.coupon_code
             END AS ALIGNER_DISCOUNT_CODE 
             FROM raw.fivetran_tesseract_public.commerce_order o
           INNER JOIN raw.fivetran_tesseract_public.commerce_order_item oi
               ON o.id = oi.order_id 
           INNER JOIN raw.fivetran_tesseract_public.commerce_product p
               ON p.id = oi.product_id
           INNER JOIN raw.fivetran_tesseract_public.commerce_product_category pc
               ON pc.id = p.product_category_id
           LEFT JOIN raw.fivetran_tesseract_public.commerce_discount disc
               ON o.discount_id = disc.id
            WHERE pc.name = 'aligner'
              AND o.date_removed IS NULL
              AND o.date_order IS NOT NULL
           ) y2
         INNER JOIN
           (SELECT MIN(o.ID) as MinRec
              FROM raw.fivetran_tesseract_public.commerce_order o
           INNER JOIN raw.fivetran_tesseract_public.commerce_order_item oi
            ON o.id = oi.order_id 
           INNER JOIN raw.fivetran_tesseract_public.commerce_product p
            ON p.id = oi.product_id
           INNER JOIN raw.fivetran_tesseract_public.commerce_product_category pc
            ON pc.id = p.product_category_id
           WHERE pc.name = 'aligner'
             AND o.date_removed IS NULL
             AND o.date_order IS NOT NULL
          GROUP BY o.USER_ID
         ) x2 ON x2.MinRec = y2.id
) ALIGNERO ON MAIN.USER_ID = ALIGNERO.USER_ID
GROUP BY STREET, SECONDARY, CITY, STATE, POSTAL, PH.PHONE, LL.EMAIL, TRANSACTION_DATE, TRANSACTION_TYPE, TRANSACTION_VALUE, REFERRAL, LOCATION, PAYMENT, FINANCED, DISCOUNT_AMOUNT, DISCOUNT_CODE, UTM_SOURCE
),
L3 AS (
        SELECT
    MIN(LL.ID) AS CUSTID, 
    PUA.STREET1 AS STREET, 
    PUA.STREET2 AS SECONDARY, 
    PUA.LOCALITY AS CITY,
    PUA.REGION AS STATE, 
    PUA.POSTAL_CODE AS POSTAL,
    PH.PHONE AS PHONE, 
    LL.EMAIL AS EMAIL, 
    KITO.DATE_ORDER AS TRANSACTION_DATE, 
    'KIT' AS TRANSACTION_TYPE, 
    KITO.KIT_SALE_PRICE AS TRANSACTION_VALUE, 
    CASE WHEN LL.KIND = 'INS' THEN
       'Insurance'
    ELSE
       'Direct'
    END AS REFERRAL, 
    'Online' AS LOCATION, 
    CASE WHEN KITO.KIT_DISCOUNT_APPLIED = 1 THEN
       'DISCOUNT'
    ELSE
       'FULL'
    END AS PAYMENT, 
    'NO' AS FINANCED,
    KITO.KIT_DISCOUNT_AMOUNT AS DISCOUNT_AMOUNT,
    KITO.KIT_DISCOUNT_CODE AS DISCOUNT_CODE,
    LASTUTM.UTM_SOURCE AS UTM_SOURCE
FROM
LL
LEFT JOIN (
           SELECT l.email, firstlc.utm_source AS UTM_SOURCE
           FROM raw.fivetran_tesseract_public.leads_lead l
           INNER JOIN
           (
               SELECT ll.email, MAX(id) AS maxRec
               FROM raw.fivetran_tesseract_public.leads_lead ll
               GROUP BY ll.email
           ) x on x.maxrec = l.id
           LEFT JOIN raw.fivetran_tesseract_public.leads_campaign firstlc ON l.campaign_id = firstlc.id
) LASTUTM ON LASTUTM.email = LL.EMAIL
INNER JOIN 
   (SELECT PU.id AS USER_ID, PU.username AS email
         FROM raw.fivetran_tesseract_public.profiles_user PU
      ) MAIN 
   ON MAIN.EMAIL = LL.EMAIL
INNER JOIN PUA ON (PUA.user_id = MAIN.user_id)
INNER JOIN raw.fivetran_tesseract_public.profiles_user_phone ph ON PH.user_id = MAIN.USER_ID
INNER JOIN 
(
    SELECT ID, USER_ID, DATE_ORDER, KIT_SALE_PRICE, KIT_DISCOUNT_APPLIED, KIT_DISCOUNT_AMOUNT, KIT_DISCOUNT_CODE
     FROM 
          (SELECT o.id, o.user_id, o.date_order, to_char( o.total_amt / 100.0, 'FM999999990.00') as KIT_SALE_PRICE, 
             CASE WHEN o.discount_amt = 0 THEN
                0
             ELSE
                1
             END AS KIT_DISCOUNT_APPLIED,
             to_char( o.discount_amt / 1.0, 'FM999999990.00' ) AS KIT_DISCOUNT_AMOUNT,
             CASE WHEN disc.coupon_code IS NULL THEN
               ''
             ELSE
               disc.coupon_code
             END AS KIT_DISCOUNT_CODE
             FROM raw.fivetran_tesseract_public.commerce_order o
           INNER JOIN raw.fivetran_tesseract_public.commerce_order_item oi
               ON o.id = oi.order_id 
           INNER JOIN raw.fivetran_tesseract_public.commerce_product p
               ON p.id = oi.product_id
           INNER JOIN raw.fivetran_tesseract_public.commerce_product_category pc
               ON pc.id = p.product_category_id
           LEFT JOIN raw.fivetran_tesseract_public.commerce_discount disc
               ON o.discount_id = disc.id
            WHERE pc.name = 'evalkit'
              AND o.date_removed IS NULL
              AND o.date_order IS NOT NULL
           ) y2
         INNER JOIN
           (SELECT MIN(o.ID) as MinRec
              FROM raw.fivetran_tesseract_public.commerce_order o
           INNER JOIN raw.fivetran_tesseract_public.commerce_order_item oi
            ON o.id = oi.order_id 
           INNER JOIN raw.fivetran_tesseract_public.commerce_product p
            ON p.id = oi.product_id
           INNER JOIN raw.fivetran_tesseract_public.commerce_product_category pc
            ON pc.id = p.product_category_id
           WHERE pc.name = 'evalkit'
             AND o.date_removed IS NULL
             AND o.date_order IS NOT NULL
          GROUP BY o.USER_ID
         ) x2 ON x2.MinRec = y2.id
) KITO ON MAIN.USER_ID = KITO.USER_ID
GROUP BY STREET, SECONDARY, CITY, STATE, POSTAL, PH.PHONE, LL.EMAIL, TRANSACTION_DATE, TRANSACTION_TYPE, TRANSACTION_VALUE, REFERRAL, LOCATION, PAYMENT, FINANCED, DISCOUNT_AMOUNT, DISCOUNT_CODE, UTM_SOURCE

),
L4 AS (
        SELECT
    MIN(LL.ID) AS CUSTID, 
    PUA.STREET1 AS STREET, 
    PUA.STREET2 AS SECONDARY, 
    PUA.LOCALITY AS CITY,
    PUA.REGION AS STATE, 
    PUA.POSTAL_CODE AS POSTAL,
    PH.PHONE AS PHONE, 
    LL.EMAIL AS EMAIL, 
    SCANO.DATE_ORDER AS TRANSACTION_DATE, 
    'SCAN' AS TRANSACTION_TYPE, 
    SCANO.SCAN_SALE_PRICE AS TRANSACTION_VALUE, 
    CASE WHEN LL.KIND = 'INS' THEN
       'Insurance'
    ELSE
       'Direct'
    END AS REFERRAL, 
    'Online' AS LOCATION, 
    CASE WHEN SCANO.SCAN_DISCOUNT_APPLIED = 1 THEN
       'DISCOUNT'
    ELSE
       'FULL'
    END AS PAYMENT, 
    'NO' AS FINANCED,
    SCANO.SCAN_DISCOUNT_AMOUNT AS DISCOUNT_AMOUNT,
    SCANO.SCAN_DISCOUNT_CODE AS DISCOUNT_CODE,
    LASTUTM.UTM_SOURCE AS UTM_SOURCE
FROM
LL
LEFT JOIN (
           SELECT l.email, firstlc.utm_source AS UTM_SOURCE
           FROM raw.fivetran_tesseract_public.leads_lead l
           INNER JOIN
           (
               SELECT ll.email, MAX(id) AS maxRec
               FROM raw.fivetran_tesseract_public.leads_lead ll
               GROUP BY ll.email
           ) x on x.maxrec = l.id
           LEFT JOIN raw.fivetran_tesseract_public.leads_campaign firstlc ON l.campaign_id = firstlc.id
) LASTUTM ON LASTUTM.email = LL.EMAIL
INNER JOIN 
   (SELECT PU.id AS USER_ID, PU.username AS email
         FROM raw.fivetran_tesseract_public.profiles_user PU
      ) MAIN 
   ON MAIN.EMAIL = LL.EMAIL
INNER JOIN PUA ON (PUA.user_id = MAIN.user_id)
INNER JOIN raw.fivetran_tesseract_public.profiles_user_phone ph ON PH.user_id = MAIN.USER_ID
INNER JOIN 
(
    SELECT ID, USER_ID, DATE_ORDER, SCAN_SALE_PRICE, SCAN_DISCOUNT_APPLIED, SCAN_DISCOUNT_AMOUNT, SCAN_DISCOUNT_CODE
     FROM 
          (SELECT o.id, o.user_id, o.date_order, to_char( o.total_amt / 100.0, 'FM999999990.00') as SCAN_SALE_PRICE, 
             CASE WHEN o.discount_amt = 0 THEN
                0
             ELSE
                1
             END AS SCAN_DISCOUNT_APPLIED,
             to_char( o.discount_amt / 1.0, 'FM999999990.00' ) AS SCAN_DISCOUNT_AMOUNT,
             CASE WHEN disc.coupon_code IS NULL THEN
               ''
             ELSE
               disc.coupon_code
             END AS SCAN_DISCOUNT_CODE
             FROM raw.fivetran_tesseract_public.commerce_order o
           INNER JOIN raw.fivetran_tesseract_public.commerce_order_item oi
               ON o.id = oi.order_id 
           INNER JOIN raw.fivetran_tesseract_public.commerce_product p
               ON p.id = oi.product_id
           INNER JOIN raw.fivetran_tesseract_public.commerce_product_category pc
               ON pc.id = p.product_category_id
           LEFT JOIN raw.fivetran_tesseract_public.commerce_discount disc
               ON o.discount_id = disc.id
            WHERE pc.name = 'scan'
              AND o.date_removed IS NULL
              AND o.date_order IS NOT NULL
           ) y2
         INNER JOIN
           (SELECT MIN(o.ID) as MinRec
              FROM raw.fivetran_tesseract_public.commerce_order o
           INNER JOIN raw.fivetran_tesseract_public.commerce_order_item oi
            ON o.id = oi.order_id 
           INNER JOIN raw.fivetran_tesseract_public.commerce_product p
            ON p.id = oi.product_id
           INNER JOIN raw.fivetran_tesseract_public.commerce_product_category pc
            ON pc.id = p.product_category_id
           WHERE pc.name = 'scan'
             AND o.date_removed IS NULL
             AND o.date_order IS NOT NULL
          GROUP BY o.USER_ID
         ) x2 ON x2.MinRec = y2.id
) SCANO ON MAIN.USER_ID = SCANO.USER_ID
GROUP BY STREET, SECONDARY, CITY, STATE, POSTAL, PH.PHONE, LL.EMAIL, TRANSACTION_DATE, TRANSACTION_TYPE, TRANSACTION_VALUE, REFERRAL, LOCATION, PAYMENT, FINANCED, DISCOUNT_AMOUNT, DISCOUNT_CODE, UTM_SOURCE

), 
X AS (
        select * FROM L1 UNION SELECT * FROM L2 UNION SELECT * FROM L3 UNION SELECT * FROM L4
)
SELECT X.CUSTID, X.STREET, X.SECONDARY, X.CITY, X.STATE, X.POSTAL, X.PHONE, X.EMAIL, X.TRANSACTION_DATE,
 X.TRANSACTION_TYPE, X.TRANSACTION_VALUE, X.REFERRAL, X.LOCATION, X.PAYMENT, X.FINANCED, X.DISCOUNT_AMOUNT, X.DISCOUNT_CODE, X.UTM_SOURCE,
 HUID.HEAP_USER_ID::text AS HEAP_USER_ID
FROM X
LEFT JOIN ANALYTICS.ANALYTICS.HEAP_USER_IDENTIFICATION HUID ON (X.EMAIL = HUID.EMAIL)

WHERE TRANSACTION_DATE >= '%s'
AND TRANSACTION_DATE < '%s'
ORDER BY TRANSACTION_DATE, CUSTID, TRANSACTION_TYPE

