create or replace table "SENDS_PII" (
    "CLIENT_ID" varchar,
    "PARTNERKEY" varchar,
    "CREATEDDATE" datetime,
    "MODIFIEDDATE" datetime,
    "ID" varchar,
    "OBJECTID" varchar,
    "EMAIL_PARTNERKEY" varchar,
    "EMAIL_ID" int,
    "SENDDATE" datetime,
    "FROMADDRESS" varchar,
    "FROMNAME" varchar,
    "DUPLICATES" int,
    "INVALIDADDRESSES" int,
    "EXISTINGUNDELIVERABLES" int,
    "EXISTINGUNSUBSCRIBES" int,
    "HARDBOUNCES" int,
    "SOFTBOUNCES" int,
    "OTHERBOUNCES" int,
    "FORWARDEDEMAILS" int,
    "UNIQUECLICKS" int,
    "UNIQUEOPENS" int,
    "NUMBERSENT" int,
    "NUMBERDELIVERED" int,
    "UNSUBSCRIBES" int,
    "MISSINGADDRESSES" int,
    "SUBJECT" varchar,
    "PREVIEWURL" varchar,
    "SENTDATE" datetime,
    "EMAILNAME" varchar,
    "STATUS" varchar,
    "ISMULTIPART"boolean,
    "ISALWAYSON"boolean,
    "NUMBERTARGETED" int,
    "NUMBERERRORED" int,
    "NUMBEREXCLUDED" int,
    "ADDITIONAL" varchar,
    "EMAILSENDDEFINITION" varchar
);


