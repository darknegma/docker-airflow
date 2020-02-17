use role ACCOUNTADMIN;
use database MARKETING;
create schema LOGIC_PLUM;

--schema level roles (new roles)
create role "MARKETING.LOGIC_PLUM.no_pii.reader.role";
create role "MARKETING.LOGIC_PLUM.no_pii.writer.role";
create role "MARKETING.LOGIC_PLUM.no_pii.owner.role";
create role "MARKETING.LOGIC_PLUM.no_pii.service.role";


--table level roles

-- BOOK_TO_SHOW_SCORES
create role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES.no_pii.table.owner";
create role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES.no_pii.table.reader";
create role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES.no_pii.table.writer";

grant ownership on table "MARKETING"."LOGIC_PLUM"."BOOK_TO_SHOW_SCORES" to role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES.no_pii.table.owner";
grant all privileges on table "MARKETING"."LOGIC_PLUM"."BOOK_TO_SHOW_SCORES" to role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES.no_pii.table.owner";

grant select on table "MARKETING"."LOGIC_PLUM"."BOOK_TO_SHOW_SCORES" to role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES.no_pii.table.reader";
grant usage on database "MARKETING" to role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES.no_pii.table.reader";
grant usage on schema "LOGIC_PLUM" to role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES.no_pii.table.reader";

grant insert,update on table "MARKETING"."LOGIC_PLUM"."BOOK_TO_SHOW_SCORES" to role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES.no_pii.table.writer";
grant usage on database "MARKETING" to role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES.no_pii.table.writer";
grant usage on schema "LOGIC_PLUM" to role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES.no_pii.table.writer";

grant role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES.no_pii.table.owner" to role "MARKETING.LOGIC_PLUM.no_pii.owner.role";

-- BOOK_TO_SHOW_SCORES_BTFT
create role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTFT.no_pii.table.owner";
create role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTFT.no_pii.table.reader";
create role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTFT.no_pii.table.writer";

grant ownership on table "MARKETING"."LOGIC_PLUM"."BOOK_TO_SHOW_SCORES_BTFT" to role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTFT.no_pii.table.owner";
grant all privileges on table "MARKETING"."LOGIC_PLUM"."BOOK_TO_SHOW_SCORES_BTFT" to role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTFT.no_pii.table.owner";

grant select on table "MARKETING"."LOGIC_PLUM"."BOOK_TO_SHOW_SCORES_BTFT" to role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTFT.no_pii.table.reader";
grant usage on database "MARKETING" to role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTFT.no_pii.table.reader";
grant usage on schema "LOGIC_PLUM" to role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTFT.no_pii.table.reader";

grant insert,update on table "MARKETING"."LOGIC_PLUM"."BOOK_TO_SHOW_SCORES_BTFT" to role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTFT.no_pii.table.writer";
grant usage on database "MARKETING" to role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTFT.no_pii.table.writer";
grant usage on schema "LOGIC_PLUM" to role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTFT.no_pii.table.writer";

grant role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTFT.no_pii.table.owner" to role "MARKETING.LOGIC_PLUM.no_pii.owner.role";

-- BOOK_TO_SHOW_SCORES_BTS_COUNTRY
create role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTS_COUNTRY.no_pii.table.owner";
create role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTS_COUNTRY.no_pii.table.reader";
create role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTS_COUNTRY.no_pii.table.writer";

grant ownership on table "MARKETING"."LOGIC_PLUM"."BOOK_TO_SHOW_SCORES_BTS_COUNTRY" to role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTS_COUNTRY.no_pii.table.owner";
grant all privileges on table "MARKETING"."LOGIC_PLUM"."BOOK_TO_SHOW_SCORES_BTS_COUNTRY" to role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTS_COUNTRY.no_pii.table.owner";

grant select on table "MARKETING"."LOGIC_PLUM"."BOOK_TO_SHOW_SCORES_BTS_COUNTRY" to role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTS_COUNTRY.no_pii.table.reader";
grant usage on database "MARKETING" to role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTS_COUNTRY.no_pii.table.reader";
grant usage on schema "LOGIC_PLUM" to role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTS_COUNTRY.no_pii.table.reader";

grant insert,update on table "MARKETING"."LOGIC_PLUM"."BOOK_TO_SHOW_SCORES_BTS_COUNTRY" to role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTS_COUNTRY.no_pii.table.writer";
grant usage on database "MARKETING" to role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTS_COUNTRY.no_pii.table.writer";
grant usage on schema "LOGIC_PLUM" to role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTS_COUNTRY.no_pii.table.writer";

grant role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTS_COUNTRY.no_pii.table.owner" to role "MARKETING.LOGIC_PLUM.no_pii.owner.role";

--owner role
grant all on schema LOGIC_PLUM to role "MARKETING.LOGIC_PLUM.no_pii.owner.role";
grant usage on database "MARKETING" to role "MARKETING.LOGIC_PLUM.no_pii.owner.role";
grant usage on schema "LOGIC_PLUM" to role "MARKETING.LOGIC_PLUM.no_pii.owner.role";
grant role "MARKETING.LOGIC_PLUM.no_pii.owner.role" to role accountadmin;

--service role
grant create table, create view, create stage on schema "MARKETING"."LOGIC_PLUM" to role "MARKETING.LOGIC_PLUM.no_pii.service.role";
grant role "MARKETING.LOGIC_PLUM.no_pii.writer.role" to role "MARKETING.LOGIC_PLUM.no_pii.service.role";
grant role "MARKETING.LOGIC_PLUM.no_pii.reader.role" to role "MARKETING.LOGIC_PLUM.no_pii.service.role";
grant role "MARKETING.LOGIC_PLUM.no_pii.service.role" to role AIRFLOW_SERVICE_ROLE;

-- Reader
grant role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES.no_pii.table.reader" to role "MARKETING.LOGIC_PLUM.no_pii.reader.role";
grant role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTFT.no_pii.table.reader" to role "MARKETING.LOGIC_PLUM.no_pii.reader.role";
grant role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTS_COUNTRY.no_pii.table.reader" to role "MARKETING.LOGIC_PLUM.no_pii.reader.role";

--Writer
grant role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES.no_pii.table.writer" to role "MARKETING.LOGIC_PLUM.no_pii.writer.role";
grant role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTFT.no_pii.table.writer" to role "MARKETING.LOGIC_PLUM.no_pii.writer.role";
grant role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTS_COUNTRY.no_pii.table.writer" to role "MARKETING.LOGIC_PLUM.no_pii.writer.role";


--DBT No PII access
grant role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES.no_pii.table.reader" to role "TRANSFORMER";
grant role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTFT.no_pii.table.reader" to role "TRANSFORMER";
grant role "MARKETING.LOGIC_PLUM.BOOK_TO_SHOW_SCORES_BTS_COUNTRY.no_pii.table.reader" to role "TRANSFORMER";
