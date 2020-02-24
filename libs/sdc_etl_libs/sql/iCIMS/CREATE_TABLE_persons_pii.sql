CREATE TABLE "HRIS_DATA"."ICIMS"."PERSONS_PII" 
(
	"FULL_NAME_FIRST_LAST" VARCHAR,
	"SYSTEM_ID" INT,
	"EMAIL" VARCHAR,
	"DEFAULT_PHONES_NUMBER" VARCHAR,
	"CREATED_DATE" DATETIME,
	"UPDATED_DATE" DATETIME,
	"DEFAULT_ADDRESSES_ADDRESS" VARCHAR,
	"DEFAULT_ADDRESSES_ADDRESS_2" VARCHAR,
	"DEFAULT_ADDRESSES_CITY" VARCHAR,
	"DEFAULT_ADDRESSES_POSTAL_CODE" VARCHAR,
	"DEFAULT_ADDRESSES_STATE_PROVINCE_FULL_NAME" VARCHAR,
	"DEFAULT_ADDRESSES_COUNTRY_FULL_NAME" VARCHAR,
	"SOURCE" VARCHAR,
	"SOURCE_CHANNEL" VARCHAR,
	"SOURCE_PORTAL" VARCHAR,
	"GENDER" VARCHAR,
	"RACE" VARCHAR,
	"PROMOTION_TRANSFER_DATE" VARCHAR,
	"SOURCE_SPECIFICS" VARCHAR,
	"SOURCE_PERSON_FULL_NAME_FIRST_LAST" VARCHAR,
	"PERSON_FOLDER" VARCHAR,
	"_ETL_FILENAME" VARCHAR,
	"_SF_INSERTEDDATETIME" DATETIME
);