CREATE OR REPLACE TABLE "MARKETING"."GOOGLE_SEARCH_ADS_360"."REPORT_ADS" COPY GRANTS
(
	"STATUS" VARCHAR,
	"ENGINESTATUS" VARCHAR,
	"CREATIONTIMESTAMP" DATETIME,
	"LASTMODIFIEDTIMESTAMP" DATETIME,
	"AGENCY" VARCHAR,
	"AGENCYID" VARCHAR,
	"ADVERTISER" VARCHAR,
	"ADVERTISERID" VARCHAR,
	"ACCOUNT" VARCHAR,
	"ACCOUNTID" VARCHAR,
	"ACCOUNTENGINEID" VARCHAR,
	"ACCOUNTTYPE" VARCHAR,
	"CAMPAIGN" VARCHAR,
	"CAMPAIGNID" VARCHAR,
	"CAMPAIGNSTATUS" VARCHAR,
	"ADGROUP" VARCHAR,
	"ADGROUPID" VARCHAR,
	"AD" VARCHAR,
	"ADID" VARCHAR,
	"ISUNATTRIBUTEDAD" BOOLEAN,
	"ADHEADLINE" VARCHAR,
	"ADTYPE" VARCHAR,
	"CLICKS" INT,
	"COST" FLOAT,
	"IMPR" INT,
	"ADWORDSCONVERSIONS" VARCHAR,
	"ADWORDSCONVERSIONVALUE" VARCHAR,
	"ADWORDSVIEWTHROUGHCONVERSIONS" INT,
	"VISITS" INT,
	"DATE" DATETIME,
	"DEVICESEGMENT" VARCHAR,
	"_SF_INSERTEDDATETIME" DATETIME
);