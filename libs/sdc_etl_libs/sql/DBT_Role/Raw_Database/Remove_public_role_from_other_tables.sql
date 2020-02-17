-- We need to remove the public role select permission on the below tables
-- We need to remove the public role ALL permission on the below SCHEMAS
-- There is a good chance that at least some if these tables are being using by the analytics team/transformer role
-- But actually only have access cause of the select on public role
-- SO the temp fix here is
-- 1.  GIve transformer select access to the below tables
-- 2.  Remove public
-- 3 . Eventually transformer will be dreprecated and replaced by the dbt roles


USE ROLE SECURITYADMIN;


-- CURRENT STATE

-- PUBLIC HAS ALL RIGHTS BUT OWNERSHIP
-- OWNERSHIP IS STITCH_ROLE
SHOW GRANTS ON SCHEMA "RAW"."SFMC_DATA_EXTENSIONS";

-- THESE TABLES SHOULD BE CONSIDERED PII
-- PUBLIC HAS SELECTS
SHOW GRANTS ON TABLE "RAW"."SFMC_DATA_EXTENSIONS"."DATA_EXTENSION_PREFERENCE_CENTER_DE";
SHOW GRANTS ON TABLE "RAW"."SFMC_DATA_EXTENSIONS"."DATA_EXTENSION_LOB_SENDS_MASTER";


-- CURRENT STATE

-- PUBLIC HAS ALL RIGHTS BUT OWNERSHIP
-- OWNERSHIP IS STITCH_ROLE
SHOW GRANTS ON SCHEMA "RAW"."STITCH_GOOGLESHEETS_SFNOTES";

-- PUBLIC HAS SELECTS
SHOW GRANTS ON TABLE "RAW"."STITCH_GOOGLESHEETS_SFNOTES"."STITCH_GOOGLESHEETS_SFNOTES";


-- FIX IT

GRANT USAGE ON SCHEMA RAW.SFMC_DATA_EXTENSIONS TO ROLE TRANSFORMER ;

GRANT SELECT ON TABLE "RAW"."SFMC_DATA_EXTENSIONS"."DATA_EXTENSION_PREFERENCE_CENTER_DE" TO ROLE TRANSFORMER ;
GRANT SELECT ON TABLE "RAW"."SFMC_DATA_EXTENSIONS"."DATA_EXTENSION_LOB_SENDS_MASTER" TO ROLE TRANSFORMER ;

REVOKE ALL ON SCHEMA RAW.SFMC_DATA_EXTENSIONS FROM ROLE PUBLIC ;

-- FIX IT

GRANT USAGE ON SCHEMA RAW.STITCH_GOOGLESHEETS_SFNOTES TO ROLE TRANSFORMER ;

GRANT SELECT ON TABLE "RAW"."STITCH_GOOGLESHEETS_SFNOTES"."STITCH_GOOGLESHEETS_SFNOTES" TO ROLE TRANSFORMER ;

REVOKE ALL ON SCHEMA RAW.STITCH_GOOGLESHEETS_SFNOTES FROM ROLE PUBLIC ;
