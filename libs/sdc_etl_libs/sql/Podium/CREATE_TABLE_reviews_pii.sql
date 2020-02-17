
use role "MARKETING.PODIUM.pii.owner.role";
use database "MARKETING";
use schema "PODIUM";

create table "REVIEWS_PII" (
	"ID" int,
	"SITEREVIEWID" varchar,
	"SITENAME" varchar,
	"AUTHORID" varchar,
	"REVIEWBODY" varchar,
	"RATING" float,
	"PUBLISHDATE" datetime,
	"REVIEWURL" varchar,
	"LOCATIONID" int,
	"AUTHOR" varchar,
	"CREATEDAT" datetime,
	"UPDATEDAT" datetime,
	"REVIEWINVITATIONID" varchar,
	"REVIEWINVITATIONUID" varchar
);

grant insert, update on table "REVIEWS_PII" to role "MARKETING.PODIUM.pii.writer.role";
grant select on table "REVIEWS_PII" to role "MARKETING.PODIUM.pii.reader.role";
