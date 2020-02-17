create table "OUTBOUND_EVENTS"
(

    "CUSTOMER_UUID"         varchar,
    "SHIP_DATE"             datetime,
    "OUTBOUND_TRACKING"     varchar,
    "KIT"                   varchar,
    "OUTBOUND_STATUS"       varchar,
    "OUTBOUND_STATUS_DATE"  datetime,
    "OUTBOUND_STATUS_CITY"  varchar,
    "OUTBOUND_STATUS_STATE" varchar,
    "_ETL_FILENAME"        varchar,
    "_SF_INSERTEDDATETIME"  datetime

);