create table "DAILY_CHARGES"
(

    "TRANSACTIONID"             varchar,
    "HFDID"                     int,
    "DATE"                      datetime,
    "ATTEMPTTYPE"               varchar,
    "AMOUNT"                    float,
    "CHARGERESPONSE"            varchar,
    "CHARGERESPONSECODE"        varchar,
    "CHARGERESPONSEDESCRIPTION" varchar,
    "MERCHANT"                  varchar,
    "_ETL_FILENAME"             varchar,
    "_SF_INSERTEDDATETIME"      datetime

);
