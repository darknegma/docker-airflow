
use role "MARKETING.PODIUM.pii.owner.role";
use database "MARKETING";
use schema "PODIUM";

create table "INVITES_PII" (
	"ID" int,
	"UID" varchar,
	"PHONENUMBER" varchar,
	"LASTINVITATIONSENT" boolean,
	"ORGANIZATIONID" int,
	"CREATEDAT" datetime,
	"UPDATEDAT" datetime,
	"REVIEWPAGEURL" varchar,
	"USERID" varchar,
	"TEST" varchar,
	"LOCATIONID" int,
	"CUSTOMERID" int,
	"EMAIL" varchar

);

grant insert, update on table "INVITES_PII" to role "MARKETING.PODIUM.pii.writer.role";
grant select on table "INVITES_PII" to role "MARKETING.PODIUM.pii.reader.role";
