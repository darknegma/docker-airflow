
--- SENSITVE INFORMATION TABLES
USE ROLE AIRFLOW_SERVICE_ROLE;
USE WAREHOUSE AIRFLOW_WAREHOUSE;
USE DATABASE HRIS_DATA;
USE SCHEMA TIMECONTROL;

CREATE OR REPLACE VIEW VW_EMPLOYEES_PII COPY GRANTS AS
(

SELECT
        BANKS	,
        STARTDATE	,
        FINISHDATE	,
        PRELOADRESOURCEASSIGNMENTS	,
        PRELOADPERSONALASSIGNMENTS	,
        DOPRELOAD	,
        ISINACTIVE	,
        LASTMODIFIEDAT	,
        LASTMODIFIEDBY	,
        RESOURCEPRELOADFILTERKEY	,
        TIMESHEETPERIODGROUP	,
        RATE	,
        RESOURCE	,
        CANCOPYTIMESHEETHOURS	,
        USERFIELDS	,
        FIRSTNAME	,
        LASTNAME	,
        _METADATA	,
        KEY	,
        CODE	,
        NAME	,
        _SF_INSERTEDDATETIME

FROM EMPLOYEES_PII
LEFT JOIN DATAENG_UTILS.MAPPINGS.PII_MAPPINGS PII ON PII.ROLE = CURRENT_ROLE()

);

------------------------

CREATE OR REPLACE VIEW VW_EMPLOYEE_REVISIONS_PII COPY GRANTS AS
(

SELECT
      STARTDATE	,
      FINISHDATE,
      RESOURCE	,
      CHANGETYPE	,
      LASTMODIFIEDAT	,
      LASTMODIFIEDBY	,
      USERFIELDS	,
      _METADATA	,
      KEY	,
      EMPLOYEEKEY	,
      CODE	,
      FIRSTNAME	,
      LASTNAME	,
      NAME	,
      _SF_INSERTEDDATETIME

FROM EMPLOYEE_REVISIONS_PII
LEFT JOIN DATAENG_UTILS.MAPPINGS.PII_MAPPINGS PII ON PII.ROLE = CURRENT_ROLE()

);

------------------------

CREATE OR REPLACE VIEW VW_TIMESHEETS_DETAILS_PII COPY GRANTS AS
(

SELECT

     -- PARSE_JSON(REPLACE(
                   --     PARSE_JSON(
                                   -- REPLACE(
                                          --  LINE, LINE:Timesheet:Employee:LastName, sha2(LINE:Timesheet:Employee:LastName,512)
                                          --  )
                                  -- ),
                      --  LINE:Timesheet:Employee:FirstName, sha2(LINE:Timesheet:Employee:FirstName,512)
                      --  )
                --) AS LINE,
      LINE,
      KEY	,
      TIMESHEETKEY,
      DATE	,
      MINUTES ,
      DAYNUMBER	,
      WEEKNUMBER	,
      HOURS	,
      TOTALHOURS	,
      _SF_INSERTEDDATETIME

FROM TIMESHEETS_DETAILS_PII
LEFT JOIN DATAENG_UTILS.MAPPINGS.PII_MAPPINGS PII ON PII.ROLE = CURRENT_ROLE()

);

-----------------------

CREATE OR REPLACE VIEW VW_TIMESHEETS_PII COPY GRANTS AS

(

SELECT
    _METADATA ,
    KEY ,
    STATUS	,
    STORE	,
  /*
    PARSE_JSON(REPLACE(
                        PARSE_JSON(
                                    REPLACE(
                                            EMPLOYEE, EMPLOYEE:LastName, sha2(EMPLOYEE:LastName,512)
                                            )
                                   ),
                         EMPLOYEE:FirstName,sha2(EMPLOYEE:FirstName,512)
                        )
                ) AS EMPLOYEE,
    PERIOD	,
    PARSE_JSON(REPLACE(
                        PARSE_JSON(
                                    REPLACE(
                                            SOURCE, SOURCE:Email, sha2(SOURCE:Email,512)
                                            )
                                   ),
                         SOURCE:Name, sha2(SOURCE:Name,512)
                        )
                ) AS SOURCE,

    PARSE_JSON(REPLACE(
                        PARSE_JSON(
                                    REPLACE(
                                            OWNER, OWNER:Email, sha2(OWNER:Email,512)
                                            )
                                   ),
                         OWNER:Name, sha2(OWNER:Name,512)
                        )
                ) AS OWNER,
*/
    EMPLOYEE,
    SOURCE,
    PERIOD	,
    OWNER,
    TOTALMINUTES	,
    TIMESHEETTYPE	,
   /*
    PARSE_JSON(REPLACE(
                        PARSE_JSON(
                                    REPLACE(
                                            LASTOWNER, LASTOWNER:Email, sha2(LASTOWNER:Email,512)
                                            )
                                   ),
                         LASTOWNER:Name, sha2(LASTOWNER:Name,512)
                        )
                ) AS LASTOWNER,
  */
    LASTOWNER,
    USERFIELDS	,
    _SF_INSERTEDDATETIME

FROM TIMESHEETS_PII
LEFT JOIN DATAENG_UTILS.MAPPINGS.PII_MAPPINGS PII ON PII.ROLE = CURRENT_ROLE()

);

------------------------

CREATE OR REPLACE VIEW VW_TIMESHEETS_POSTED_DETAILS_PII COPY GRANTS AS
(

SELECT
      BATCH	,
      COSTS	,
      PREMIUMCOSTS	,
      FLAGS	,
      /*
      PARSE_JSON(REPLACE(
                        PARSE_JSON(
                                    REPLACE(
                                            LINE, LINE:Timesheet:Employee:LastName, sha2(LINE:Timesheet:Employee:LastName,512)
                                            )
                                   ),
                        LINE:Timesheet:Employee:FirstName, sha2(LINE:Timesheet:Employee:FirstName,512)
                        )
                ) AS LINE,
     */
      LINE,
      KEY	,
      TIMESHEETKEY	,
      DATE	,
      MINUTES	,
      DAYNUMBER	,
      WEEKNUMBER	,
      HOURS	,
      TOTALHOURS	,
      _SF_INSERTEDDATETIME

FROM TIMESHEETS_POSTED_DETAILS_PII
LEFT JOIN DATAENG_UTILS.MAPPINGS.PII_MAPPINGS PII ON PII.ROLE = CURRENT_ROLE()

);

------------------------

CREATE OR REPLACE VIEW VW_TIMESHEETS_POSTED_PII COPY GRANTS AS
(

SELECT
      _METADATA	,
      KEY	,
      STATUS	,
      STORE	,
      /*
      PARSE_JSON(REPLACE(
                          PARSE_JSON(
                                      REPLACE(
                                              EMPLOYEE, EMPLOYEE:LastName, sha2(EMPLOYEE:LastName,512)
                                              )
                                     ),
                           EMPLOYEE:FirstName, sha2( EMPLOYEE:FirstName,512)
                          )
                  ) AS EMPLOYEE,
      */
      EMPLOYEE,
      PERIOD	,
      TOTALMINUTES	,
      TIMESHEETTYPE	,
      USERFIELDS	,
      _SF_INSERTEDDATETIME

FROM  TIMESHEETS_POSTED_PII
LEFT JOIN DATAENG_UTILS.MAPPINGS.PII_MAPPINGS PII ON PII.ROLE = CURRENT_ROLE()

);


------------------------

CREATE OR REPLACE VIEW VW_USERS_PII COPY GRANTS AS
(

SELECT
      LANGUAGE	,
      PROFILE	,
      EMAIL	,
      ISINACTIVE	,
      LOGINTYPE	,
      MOBILELOGINTYPE	,
      ACTIVEDIRUSER	,
      LASTMODIFIEDAT	,
      LASTMODIFIEDBY	,
      USERFIELDS	,
      _METADATA	,
      KEY	,
      CODE	,
      NAME	,
      /*
      PARSE_JSON(REPLACE(
                          PARSE_JSON(
                                      REPLACE(
                                              EMPLOYEE, EMPLOYEE:LastName, sha2(EMPLOYEE:LastName,512)
                                              )
                                     ),
                           EMPLOYEE:FirstName,sha2(EMPLOYEE:FirstName,512)
                          )
                  ) AS EMPLOYEE,
      */
      EMPLOYEE,
      _SF_INSERTEDDATETIME

FROM  USERS_PII
LEFT JOIN DATAENG_UTILS.MAPPINGS.PII_MAPPINGS PII ON PII.ROLE = CURRENT_ROLE()

);


------ ADD ROLES
-- 1. FOR THE PII OR SENSITIVE DATA WE CREATED VIEW FOR.. CREATE A NEW NO_PII READER ROLE AND GROUP ROLE
-- 2. FOR THE TABLE THAT DO NOT HAVE PII OR SENISITIVE DATA - USE THE CURRENT READER ROE AND ADD THOSE TO THE GROUP ROLE

USE ROLE SECURITYADMIN;

-- individual view readers

-- VW_EMPLOYEES_PII
CREATE ROLE "HRIS_DATA.TIMECONTROL.EMPLOYEES_PII.no_pii.view.reader";
grant select on view "HRIS_DATA"."TIMECONTROL"."VW_EMPLOYEES_PII" to role "HRIS_DATA.TIMECONTROL.EMPLOYEES_PII.no_pii.view.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.EMPLOYEES_PII.no_pii.view.reader";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.EMPLOYEES_PII.no_pii.view.reader";
grant role "HRIS_DATA.TIMECONTROL.EMPLOYEES_PII.no_pii.view.reader" to role "HRIS_DATA.TIMECONTROL.no_pii.reader.role";

-- VW_EMPLOYEE_REVISIONS_PII
CREATE ROLE "HRIS_DATA.TIMECONTROL.EMPLOYEE_REVISIONS_PII.no_pii.view.reader";
grant select on view "HRIS_DATA"."TIMECONTROL"."VW_EMPLOYEE_REVISIONS_PII" to role "HRIS_DATA.TIMECONTROL.EMPLOYEE_REVISIONS_PII.no_pii.view.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.EMPLOYEE_REVISIONS_PII.no_pii.view.reader";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.EMPLOYEE_REVISIONS_PII.no_pii.view.reader";
grant role "HRIS_DATA.TIMECONTROL.EMPLOYEE_REVISIONS_PII.no_pii.view.reader" to role "HRIS_DATA.TIMECONTROL.no_pii.reader.role";

-- VW_TIMESHEETS_DETAILS_PII
CREATE ROLE "HRIS_DATA.TIMECONTROL.TIMESHEETS_DETAILS_PII.no_pii.view.reader";
grant select on view "HRIS_DATA"."TIMECONTROL"."VW_TIMESHEETS_DETAILS_PII" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_DETAILS_PII.no_pii.view.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_DETAILS_PII.no_pii.view.reader";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_DETAILS_PII.no_pii.view.reader";
grant role  "HRIS_DATA.TIMECONTROL.TIMESHEETS_DETAILS_PII.no_pii.view.reader" to role "HRIS_DATA.TIMECONTROL.no_pii.reader.role";

-- VW_TIMESHEETS_PII
CREATE ROLE "HRIS_DATA.TIMECONTROL.TIMESHEETS_PII.no_pii.view.reader";
grant select on view "HRIS_DATA"."TIMECONTROL"."VW_TIMESHEETS_PII" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_PII.no_pii.view.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_PII.no_pii.view.reader";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_PII.no_pii.view.reader";
grant role  "HRIS_DATA.TIMECONTROL.TIMESHEETS_PII.no_pii.view.reader" to role "HRIS_DATA.TIMECONTROL.no_pii.reader.role";

-- VW_TIMESHEETS_POSTED_DETAILS_PII
CREATE ROLE "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_DETAILS_PII.no_pii.view.reader";
grant select on view "HRIS_DATA"."TIMECONTROL"."VW_TIMESHEETS_POSTED_DETAILS_PII" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_DETAILS_PII.no_pii.view.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_DETAILS_PII.no_pii.view.reader";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_DETAILS_PII.no_pii.view.reader";
grant role  "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_DETAILS_PII.no_pii.view.reader" to role "HRIS_DATA.TIMECONTROL.no_pii.reader.role";

-- VW_TIMESHEETS_POSTED_PII
CREATE ROLE "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_PII.no_pii.view.reader";
grant select on view "HRIS_DATA"."TIMECONTROL"."VW_TIMESHEETS_POSTED_PII" to role  "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_PII.no_pii.view.reader";
grant usage on database "HRIS_DATA" to role  "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_PII.no_pii.view.reader";
grant usage on schema "TIMECONTROL" to role  "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_PII.no_pii.view.reader";
grant role  "HRIS_DATA.TIMECONTROL.TIMESHEETS_POSTED_PII.no_pii.view.reader" to role "HRIS_DATA.TIMECONTROL.no_pii.reader.role";

-- VW_TIMESHEETS_POSTED_PII
CREATE ROLE "HRIS_DATA.TIMECONTROL.USERS_PII.no_pii.view.reader";
grant select on view "HRIS_DATA"."TIMECONTROL"."VW_USERS_PII" to role "HRIS_DATA.TIMECONTROL.USERS_PII.no_pii.view.reader";
grant usage on database "HRIS_DATA" to role "HRIS_DATA.TIMECONTROL.USERS_PII.no_pii.view.reader";
grant usage on schema "TIMECONTROL" to role "HRIS_DATA.TIMECONTROL.USERS_PII.no_pii.view.reader";
grant role  "HRIS_DATA.TIMECONTROL.USERS_PII.no_pii.view.reader" to role "HRIS_DATA.TIMECONTROL.no_pii.reader.role";


-- ADD TABLES THAT DO NOT HAVE PII OR SENSITIVE INFORMATION (THE ONES WE DID NOT CREATE VIEWS FOR)
-- MOST OF THESE WERE ALREADY ADDED TO THE NO-PII Reader role
-- THIS TABLE HOWEVER REALLY DOES NOT HAVE PII OR SENSITIVE DATA
GRANT ROLE "HRIS_DATA.TIMECONTROL.RATES_PII.pii.table.reader" TO  ROLE "HRIS_DATA.TIMECONTROL.no_pii.reader.role";


GRANT ROLE "HRIS_DATA.TIMECONTROL.no_pii.reader.role" TO ROLE DBT_DEV;
--TEMP UNTIL QE UNHOOK TRANSFORMER
GRANT ROLE DBT_DEV TO ROLE TRANSFORMER;

SHOW GRANTS TO ROLE DBT_DEV

-------############################################################

-- TEST AND JUNK STUFF BELOW ----------------------------

------###############################################################



--REVOKE SELECT ON VIEW  "HRIS_DATA"."ULTIPRO"."VW_TIME_MANAGEMENT_TIME_PII" FROM ROLE DBT_DEV

SHOW GRANTS TO ROLE   "HRIS_DATA.TIMECONTROL.pii.reader.role"
show GRANTS TO ROLE "HRIS_DATA.TIMECONTROL.RATES_PII.pii.table.reader"

show grants on schema TIMECONTROL;
SHOW GRANTS on ROLE "HRIS_DATA.TIMECONTROL.no_pii.reader.role"

SHOW GRANTS TO ROLE "HRIS_DATA.TIMECONTROL.no_pii.reader.role" --> HAS DIRECT SELECT PERMISSION ON THE NOPII TABLES.. NOT A NO PII ROLE
SHOW GRANTS TO ROLE "HRIS_DATA.TIMECONTROL.pii.reader.role" -- HAS BOTH ON THE INDIVIDUA TABLE READER ROLES BUT ALSO DIRECTLY TO THE TABLE ??

SHOW GRANTS TO ROLE "HRIS_DATA.TIMECONTROL.CHARGES.pii.table.reader"

SHOW GRANTS ON TABLE
HRIS_DATA.TIMECONTROL.PERIODS_PAY

--TESTING
SELECT * FROM VW_EMPLOYEES_PII            LIMIT 10;
SELECT * FROM VW_EMPLOYEE_REVISIONS_PII   LIMIT 10;
SELECT * FROM VW_TIMESHEETS_DETAILS_PII   LIMIT 10;
SELECT * FROM VW_TIMESHEETS_PII           LIMIT 10;
SELECT * FROM VW_TIMESHEETS_POSTED_DETAILS_PII           LIMIT 10;
SELECT * FROM VW_TIMESHEETS_POSTED_PII           LIMIT 10;
SELECT * FROM VW_USERS_PII           LIMIT 10;



-- TABLES WE DID NOT CREATE VIEW FOR
-- GET CURRENT READER
SHOW GRANTS ON TABLE CHARGES;
SHOW GRANTS ON TABLE CHARGE_REVISIONS;
SHOW GRANTS ON TABLE LANGUAGES;
SHOW GRANTS ON TABLE PERIODS_PAY;
SHOW GRANTS ON TABLE PERIODS_TIMESHEET;
SHOW GRANTS ON TABLE PROJECTS;
SHOW GRANTS ON TABLE RATES_PII;
SHOW GRANTS ON TABLE REPORTS;
SHOW GRANTS ON TABLE RESOURCES;
SHOW GRANTS ON TABLE WBS;


