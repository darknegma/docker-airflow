CREATE TABLE "MARKETING"."DATAXU"."DAILY_IMPRESSIONS"
(
	"DATE" DATETIME,
	"ADVERTISER_NAME" VARCHAR,
	"CAMPAIGN_NAME" VARCHAR,
	"FLIGHT_UID" VARCHAR,
	"FLIGHT_NAME" VARCHAR,
	"CREATIVE_UID" VARCHAR,
	"CREATIVE_NAME" VARCHAR,
	"SPEND" FLOAT,
	"IMPRESSIONS" INT,
	"CLICKS" INT,
	"LEAD_FORM_MARKETING_LAST_VIEW" INT,
	"LEAD_FORM_MARKETING_LAST_CLICK" INT,
	"IMPRESSION_KIT_CONVERSION_5_8_17_LAST_VIEW" INT,
	"IMPRESSION_KIT_CONVERSION_5_8_17_LAST_CLICK" INT,
	"BOOK_AN_APPT_LAST_VIEW" INT,
	"BOOK_AN_APPT_LAST_CLICK" INT,
	"_ETL_FILENAME" VARCHAR,
	"_SF_INSERTEDDATETIME" DATETIME
);