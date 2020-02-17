create table "INBOUND_EVENTS"
(

    "CUSTOMER_UUID"        varchar,
    "SHIP_DATE"            datetime,
    "USPS_RETURN_TRACKING" varchar,
    "KIT"                  varchar,
    "INBOUND_STATUS"       varchar,
    "INBOUND_STATUS_DATE"  datetime,
    "INBOUND_STATUS_CITY"  varchar,
    "INBOUND_STATUS_STATE" varchar,
    "_ETL_FILENAME"        varchar,
    "_SF_INSERTEDDATETIME" datetime

);

