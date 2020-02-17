
use role "MARKETING.PODIUM.no_pii.owner.role";
use database "MARKETING";
use schema "PODIUM";

create table "LOCATIONS" (
	"LOCATIONID" int,
	"LOCATIONNAME" varchar
);

grant insert, update on table "LOCATIONS" to role "MARKETING.PODIUM.no_pii.writer.role";
grant select on table "LOCATIONS" to role "MARKETING.PODIUM.no_pii.reader.role";
