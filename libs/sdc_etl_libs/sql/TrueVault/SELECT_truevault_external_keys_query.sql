

SELECT

    ID as CASE_ID,
    EXTERNAL_KEY

FROM "RAW"."FIVETRAN_TESSERACT_PUBLIC"."CASES_PHOTO_ASSESSMENT_ASSET_DATASTORE"

WHERE DATE_CREATED >= '{0}'
AND DATE_CREATED < '{1}';