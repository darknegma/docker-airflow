use role "MARKETING.NEUSTAR.no_pii.owner.role";
use database "MARKETING";
use schema "NEUSTAR";

CREATE OR REPLACE TABLE "FACT_FUNNEL"
(
      UNATTRIBUTED_REV    FLOAT,
      UNATTRIBUTED_QTY    FLOAT,
      UNATTRIBUTED_COUNT  FLOAT,
      FULL_IA_COUNT       FLOAT,
      IA_COUNT            FLOAT,
      FULL_IA_REV         FLOAT,
      IA_REV              FLOAT,
      FULL_IA_QTY         FLOAT,
      IA_QTY              FLOAT,
      ASSIST_COUNT        FLOAT,
      DIRECT_COUNT        FLOAT,
      ACTIVITY_ROW_COUNT  FLOAT,
      TOTAL_REVENUE       FLOAT,
      TOTAL_QUANTITY      FLOAT,
      EVENT_ROW_COUNT     FLOAT,
      TOTAL_SPEND         FLOAT,
      CUSTOM_MEASURE      INTEGER,
      ACT_YEAR            INTEGER,
      ACT_MONTH           INTEGER,
      ACT_WEEK_START      STRING,
      ACT_WEEK_END        STRING,
      ACT_WEEK            INTEGER,
      ACT_DATE            STRING,
      ACT_DAY_OF_WEEK     INTEGER,
      EVT_YEAR            INTEGER,
      EVT_MONTH           INTEGER,
      EVT_WEEK_START      STRING,
      EVT_WEEK_END        STRING,
      EVT_WEEK            INTEGER,
      EVT_QUARTER         INTEGER,
      EVT_DATE            STRING,
      EVT_DAY_OF_WEEK     INTEGER,
      ACTIVITY_TYPE       STRING,
      ACT_CHANNEL         STRING,
      ACT_E1_SEGMENT      STRING,
      ACT_CUSTOM1         STRING,
      ACT_CUSTOM2         STRING,
      ACT_CUSTOM3         STRING,
      ACT_CUSTOM4         STRING,
      ACT_CUSTOM5         STRING,
      ACT_CUSTOM6         STRING,
      ACT_USER_SEGMENT    STRING,
      EVT_TYPE_CLASS      STRING,
      EVT_CAMPAIGN_KEY    STRING,
      EVT_PLACEMENT_KEY   STRING,
      EVT_CREATIVE_KEY    STRING,
      EVT_KEYWORD         STRING,
      EVT_CUSTOM3         STRING,
      EVT_SOURCE          STRING,
      EVT_E1_SEGMENT      STRING,
      AD_BUY_PLACEMENT    STRING,
      GEO                 STRING,
      AD_FORMAT           STRING,
      BUY_METHOD          STRING,
      AD_BUY_CHANNEL      STRING,
      DEVICE              STRING,
      TACTIC              STRING,
      TARGETING_TYPE      STRING,
      TARGETING_CATEGORY  STRING,
      TARGETING_SEGMENT   STRING,
      AGE                 STRING,
      GENDER              STRING,
      FUNNEL              STRING,
      EVT_USER_SEGMENT    STRING,
      MEDIA_CHANNEL       STRING,
      EVT_PUBLISHER_NAME  STRING,
      EVT_PLACEMENT_NAME  STRING,
      EVT_GEO_NAME        STRING,
      EVT_DEVICE_TYPE_NAME STRING,
      EVT_CREATIVE_NAME   STRING,
      EVT_CAMPAIGN_NAME   STRING,
      EVT_ADVERTISER_NAME STRING,
      EVT_PLACEMENT_ID    STRING,
      EVT_CAMPAIGN_ID     STRING,
      EVT_PUBLISHER_ID    STRING,
      EVT_CREATIVE_ID     STRING,
      EVT_POST_VISIT      STRING,
      REPORTTYPE          STRING,
      FILE_ID             INTEGER
);

grant SELECT, insert, update on table "FACT_FUNNEL" to role "MARKETING.NEUSTAR.no_pii.writer.role";
grant select on table "FACT_FUNNEL" to role "MARKETING.NEUSTAR.no_pii.reader.role";

grant role "MARKETING.NEUSTAR.no_pii.reader.role" to role TRANSFORMER;

