-- UPDATE PLAN
-- 11 TABLES ARE BEING USED BY DBT
-- 7 NO_PII TABLES - CREATE THE INDIVIDUAL READ ROLES FOR THESE
-- 4 PII TABLE - THERE ALREADY IS A PII_READER SET UP FOR MOST OF THESE TABLES - "AIRFLOW_WAREHOUSE.RAW.AIRFLOW_HFD.pii.reader.role"
-- CREATE THE 4 VIEW FOR THE PII TABLE. CREATE A READ ROLE FOR IT
-- ADD THE 7 TABLES AND 4 VIEW ROLES TO THE NO_PII GROUP ROLE
-- ADD THE NO_PII GROUP ROLE TO BOTH DBT ROLES
-- NOTE - THIS NO PII ROLE HAS ALREADY BEEN SET UP - BUT HAS NO PRIVILEGES SET TO IT - "AIRFLOW_WAREHOUSE.RAW.HFD.no_pii.reader.role"
--
-- NOTE TRANSFORMER ROLE HAS OWNERSHIP OF MANY TABLES HERE AND WILL TAKE SOME WORK TO DE-HOOK ALL ITS RIGHTS HERE
-- BUT EVENTUALLY THIS SHOULD BE REPLACED BY THE FINANCE DATAMART SO MAY JUST LET IT DIE NATURALLY

--RAW.AIRFLOW_HFD.AGING
--RAW.AIRFLOW_HFD.AMORTIZATION_REPORT
--RAW.AIRFLOW_HFD.CUSTOMER_ADJUSTED_TERM_LENGTHS
--RAW.AIRFLOW_HFD.CUSTOMER_PORTAL_ACCESS_LAST60
--RAW.AIRFLOW_HFD.CUSTOMERS_WITHOUT_PATRON_ACCOUNT
--RAW.AIRFLOW_HFD.DOCD_AND_ACTIVATED
--RAW.AIRFLOW_HFD.MISSED_AND_MANUAL_PAYMENTS
--RAW.AIRFLOW_HFD.OPEN_APPS
--RAW.AIRFLOW_HFD.PAYMENT_REPORT
--RAW.AIRFLOW_HFD.PAYMENT_REPORT_SUBMITTED_FUNDS
--RAW.AIRFLOW_HFD.SUMMARY_AND_DETAIL

USE ROLE AIRFLOW_SERVICE_ROLE;
USE DATABASE RAW;
USE SCHEMA AIRFLOW_HFD;

CREATE OR REPLACE VIEW VW_CUSTOMERS_WITHOUT_PATRON_ACCOUNT  COPY GRANTS  AS
(
  SELECT
      APPLICATIONID ,
      CASE WHEN PII_ACCESS = TRUE THEN EMAIL ELSE sha2(EMAIL,512)  END AS EMAIL,
      ACTIVATEDON	 ,
      PAYMENTSTATUS	 ,
      PROCESSED_TIME

  FROM CUSTOMERS_WITHOUT_PATRON_ACCOUNT
  LEFT JOIN DATAENG_UTILS.MAPPINGS.PII_MAPPINGS PII ON PII.ROLE = CURRENT_ROLE()
);


CREATE OR REPLACE VIEW VW_DOCD_AND_ACTIVATED  COPY GRANTS  AS
(
  SELECT
      DOCDON ,
      ACTIVATEDON	,
      PATIENTID	,
      INVOICETOTAL ,
      TOTALDOWNPAYMENT ,
      FINANCEAMOUNT ,
      BEGINNINGPRINCIPALAMOUNT ,
      DOCSTAMPTAX ,
      STATUS	,
      HFD_ID	,
      CASE WHEN PII_ACCESS = TRUE THEN EMAIL ELSE sha2(EMAIL,512)  END AS EMAIL,
      CASE WHEN PII_ACCESS = TRUE THEN CUSTOMERNAME ELSE sha2(CUSTOMERNAME,512)  END AS CUSTOMERNAME,
      PROVIDERID	,
      PROCESSED_TIME

  FROM DOCD_AND_ACTIVATED
  LEFT JOIN DATAENG_UTILS.MAPPINGS.PII_MAPPINGS PII ON PII.ROLE = CURRENT_ROLE()
);


CREATE OR REPLACE VIEW VW_OPEN_APPS COPY GRANTS  AS
(
  SELECT
        HFD_ID	,
        PATIENTID	,
        CASE WHEN PII_ACCESS = TRUE THEN CUSTOMERNAME ELSE sha2(CUSTOMERNAME,512)  END AS CUSTOMERNAME,
        CASE WHEN PII_ACCESS = TRUE THEN PERSONRECEIVINGSERVICENAME ELSE sha2(PERSONRECEIVINGSERVICENAME,512)  END AS PERSONRECEIVINGSERVICENAME,
        SUBMITTEDON	,
        CREATEDON	,
        STATUS	,
        PROVIDERID	,
        PROCESSED_TIME

  FROM OPEN_APPS
  LEFT JOIN DATAENG_UTILS.MAPPINGS.PII_MAPPINGS PII ON PII.ROLE = CURRENT_ROLE()
);


CREATE OR REPLACE VIEW VW_SUMMARY_AND_DETAIL  COPY GRANTS  AS
(
  SELECT
        DOCSTAMPTAX	,
        DOCDYM	,
        DOCDDATE	,
        DOCWEEK	,
        CONFPROCEDUREDATE	,
        CONFDATEYM	,
        ACTIVATEDONYM	,
        LASTPAYMENTDATE ,
        NEXTPAYMENTDATE ,
        SCHPMT	,
        PMTDAOFMTH	,
        PAYMENTSTATUS	,
        ACTIVATEDON	,
        CURRENTMONTHACTIVATIONS	,
        ORIGINALPRINCIPAL	,
        TOTALPRINCIPALAMOUNTCOLLECTED	,
        WRITEOFFAMOUNT	,
        WRITEOFFINTERESTAMOUNT	,
        ADJUSTMENTAMOUNT	,
        CANCELEDAMOUNT	,
        ENDINGPRINCIPALBALANCE	,
        ACCRUEDINTEREST	,
        FUTUREINTEREST	,
        INTERESTPAID	,
        OUTSTANDINGACCUREDINTEREST	,
        TOTALPAYMENTS	,
        ENDINGARBALANCE	,
        TERM	,
        CURRENTMONTHLYAMOUNTOWED	,
        ACCURREDTHROUGHENDOFMONTH	,
        CURRENTMONTHACCRUEDINTEREST	,
        HFDID	,
        PATIENTID	,
        CASE WHEN PII_ACCESS = TRUE THEN CUSTOMERNAME ELSE  sha2(CUSTOMERNAME,512) END AS CUSTOMERNAME,
        CASE WHEN PII_ACCESS = TRUE THEN PERSONRECEIVINGSERVICENAME ELSE  sha2(PERSONRECEIVINGSERVICENAME,512) END AS PERSONRECEIVINGSERVICENAME,
        ADJUSTMENTDATE	,
        WRITEOFFDATE	,
        CANCELEDDATE	,
        CURRENTMONTHADJUSTMENT	,
        CURRENTMONTHWRITEOFF	,
        CURRENTMONTHWRITEOFFINTERESTAMOUNT	,
        CURRENTMONTHCANCELATION ,
        CURRENTMONTHWAC	,
        WAC ,
        CURRENTMONTHPRINCIPALAMOUNTPAID	,
        CURRENTMONTHINTERESTAMOUNTPAID	,
        ORIGINATINGSTATE	,
        DETAILSTATUS	,
        TOTALDOWNPAYMENT	,
        TOTALPROCEDURECOST	,
        COUNTPASTDUEACCTS	,
        AMOUNTFINANCED ,
        TOTALINTEREST	,
        TOTALPAYMENTDUE	,
        INTERESTRATE	,
        MOSTRECENTAGINGBUCKET	,
        EXPECTEDPAYMENTS	,
        PMTMINUSEXP	,
        PMTAHEADOFSCH	,
        UPDATEDPAYMENTSTATUS	,
        MISSEDPMT	,
        CUREDACCTS	,
        CURRENTACCTS	,
        HASBEENCHARGED	,
        PROVIDERID	,
        PROCESSED_TIME

  FROM SUMMARY_AND_DETAIL
  LEFT JOIN DATAENG_UTILS.MAPPINGS.PII_MAPPINGS PII ON PII.ROLE = CURRENT_ROLE()
);

USE ROLE SECURITYADMIN;

-- RENANE THE CURRENT NO PII ROLE TO MAKE IN LINE WITH OUR NAMING COVENTION
ALTER ROLE "AIRFLOW_WAREHOUSE.RAW.HFD.no_pii.reader.role" RENAME TO "RAW.AIRFLOW_HFD.no_pii.reader.role";

GRANT USAGE ON DATABASE RAW TO ROLE "RAW.AIRFLOW_HFD.no_pii.reader.role";
GRANT USAGE ON SCHEMA RAW.AIRFLOW_HFD TO ROLE "RAW.AIRFLOW_HFD.no_pii.reader.role" ;


-- CREATE THE INDIVIDUAL TABLE ROLES AND VIEW ROLEs
CREATE ROLE "RAW.AIRFLOW_HFD.AGING.no_pii.table.reader";
CREATE ROLE "RAW.AIRFLOW_HFD.AMORTIZATION_REPORT.no_pii.table.reader";
CREATE ROLE "RAW.AIRFLOW_HFD.CUSTOMER_ADJUSTED_TERM_LENGTHS.no_pii.table.reader";
CREATE ROLE "RAW.AIRFLOW_HFD.CUSTOMER_PORTAL_ACCESS_LAST60.no_pii.table.reader";
CREATE ROLE "RAW.AIRFLOW_HFD.MISSED_AND_MANUAL_PAYMENTS.no_pii.table.reader";
CREATE ROLE "RAW.AIRFLOW_HFD.PAYMENT_REPORT.no_pii.table.reader";
CREATE ROLE "RAW.AIRFLOW_HFD.PAYMENT_REPORT_SUBMITTED_FUNDS.no_pii.table.reader";

CREATE ROLE "RAW.AIRFLOW_HFD.CUSTOMERS_WITHOUT_PATRON_ACCOUNT.no_pii.view.reader";
CREATE ROLE "RAW.AIRFLOW_HFD.DOCD_AND_ACTIVATED.no_pii.view.reader";
CREATE ROLE "RAW.AIRFLOW_HFD.OPEN_APPS.no_pii.view.reader";
CREATE ROLE "RAW.AIRFLOW_HFD.SUMMARY_AND_DETAIL.no_pii.view.reader";

-- ADD THE INVIDUAL ROLES TO THIER TABLES/VIEW
GRANT SELECT ON TABLE "RAW"."AIRFLOW_HFD"."AGING" TO ROLE "RAW.AIRFLOW_HFD.AGING.no_pii.table.reader";
GRANT SELECT ON TABLE "RAW"."AIRFLOW_HFD"."AMORTIZATION_REPORT" TO ROLE "RAW.AIRFLOW_HFD.AMORTIZATION_REPORT.no_pii.table.reader";
GRANT SELECT ON TABLE "RAW"."AIRFLOW_HFD"."CUSTOMER_ADJUSTED_TERM_LENGTHS" TO ROLE "RAW.AIRFLOW_HFD.CUSTOMER_ADJUSTED_TERM_LENGTHS.no_pii.table.reader";
GRANT SELECT ON TABLE "RAW"."AIRFLOW_HFD"."CUSTOMER_PORTAL_ACCESS_LAST60" TO ROLE "RAW.AIRFLOW_HFD.CUSTOMER_PORTAL_ACCESS_LAST60.no_pii.table.reader";
GRANT SELECT ON TABLE "RAW"."AIRFLOW_HFD"."MISSED_AND_MANUAL_PAYMENTS" TO ROLE "RAW.AIRFLOW_HFD.MISSED_AND_MANUAL_PAYMENTS.no_pii.table.reader";
GRANT SELECT ON TABLE "RAW"."AIRFLOW_HFD"."PAYMENT_REPORT" TO ROLE "RAW.AIRFLOW_HFD.PAYMENT_REPORT.no_pii.table.reader";
GRANT SELECT ON TABLE "RAW"."AIRFLOW_HFD"."PAYMENT_REPORT_SUBMITTED_FUNDS" TO ROLE "RAW.AIRFLOW_HFD.PAYMENT_REPORT_SUBMITTED_FUNDS.no_pii.table.reader";

GRANT SELECT ON VIEW "RAW"."AIRFLOW_HFD"."VW_CUSTOMERS_WITHOUT_PATRON_ACCOUNT" TO ROLE "RAW.AIRFLOW_HFD.CUSTOMERS_WITHOUT_PATRON_ACCOUNT.no_pii.view.reader";
GRANT SELECT ON VIEW "RAW"."AIRFLOW_HFD"."VW_DOCD_AND_ACTIVATED" TO ROLE "RAW.AIRFLOW_HFD.DOCD_AND_ACTIVATED.no_pii.view.reader";
GRANT SELECT ON VIEW "RAW"."AIRFLOW_HFD"."VW_OPEN_APPS" TO ROLE "RAW.AIRFLOW_HFD.OPEN_APPS.no_pii.view.reader";
GRANT SELECT ON VIEW "RAW"."AIRFLOW_HFD"."VW_SUMMARY_AND_DETAIL" TO ROLE "RAW.AIRFLOW_HFD.SUMMARY_AND_DETAIL.no_pii.view.reader";

-- ADD THE INVIDUAL ROLES TO ITS GrOUP ROLES
GRANT ROLE  "RAW.AIRFLOW_HFD.AGING.no_pii.table.reader" TO ROLE "RAW.AIRFLOW_HFD.no_pii.reader.role" ;
GRANT ROLE  "RAW.AIRFLOW_HFD.AMORTIZATION_REPORT.no_pii.table.reader" TO ROLE "RAW.AIRFLOW_HFD.no_pii.reader.role" ;
GRANT ROLE  "RAW.AIRFLOW_HFD.CUSTOMER_ADJUSTED_TERM_LENGTHS.no_pii.table.reader" TO ROLE "RAW.AIRFLOW_HFD.no_pii.reader.role" ;
GRANT ROLE  "RAW.AIRFLOW_HFD.CUSTOMER_PORTAL_ACCESS_LAST60.no_pii.table.reader" TO ROLE "RAW.AIRFLOW_HFD.no_pii.reader.role" ;
GRANT ROLE  "RAW.AIRFLOW_HFD.MISSED_AND_MANUAL_PAYMENTS.no_pii.table.reader" TO ROLE "RAW.AIRFLOW_HFD.no_pii.reader.role" ;
GRANT ROLE  "RAW.AIRFLOW_HFD.PAYMENT_REPORT.no_pii.table.reader" TO ROLE "RAW.AIRFLOW_HFD.no_pii.reader.role" ;
GRANT ROLE  "RAW.AIRFLOW_HFD.PAYMENT_REPORT_SUBMITTED_FUNDS.no_pii.table.reader" TO ROLE "RAW.AIRFLOW_HFD.no_pii.reader.role" ;

GRANT ROLE "RAW.AIRFLOW_HFD.CUSTOMERS_WITHOUT_PATRON_ACCOUNT.no_pii.view.reader" TO ROLE "RAW.AIRFLOW_HFD.no_pii.reader.role" ;
GRANT ROLE "RAW.AIRFLOW_HFD.DOCD_AND_ACTIVATED.no_pii.view.reader" TO ROLE "RAW.AIRFLOW_HFD.no_pii.reader.role" ;
GRANT ROLE "RAW.AIRFLOW_HFD.OPEN_APPS.no_pii.view.reader"  TO ROLE "RAW.AIRFLOW_HFD.no_pii.reader.role" ;
GRANT ROLE "RAW.AIRFLOW_HFD.SUMMARY_AND_DETAIL.no_pii.view.reader" TO ROLE "RAW.AIRFLOW_HFD.no_pii.reader.role" ;

-- ADD GROUP ROLES TO THE DBT ROLE
GRANT ROLE "RAW.AIRFLOW_HFD.no_pii.reader.role"  TO ROLE DBT_DEV;
GRANT ROLE "RAW.AIRFLOW_HFD.no_pii.reader.role"  TO ROLE DBT_PROD;


-------############################################################

-- TEST AND JUNK STUFF BELOW ----------------------------

------###############################################################

USE WAREHOUSE TRANSFORMING_WAREHOUSE;
USE DATABASE RAW;
USE SCHEMA AIRFLOW_HFD;


-- TRANSFORMER ROLE HERE HAS ALL RIGHTS ON THE SCHEMA
show grants on schema AIRFLOW_HFD;

-- AND OWNS MANY OF THE TABLES HERE
-- EXAMPLES
show grants on table RAW.AIRFLOW_hfd.customer_card_info ;
show grants on table RAW.AIRFLOW_hfd.AGING;
show grants on table RAW.AIRFLOW_hfd.DOCD_AND_ACTIVATED;

show grants to ROLE "AIRFLOW_WAREHOUSE.RAW.AIRFLOW_HFD.pii.reader.role" ;
show grants ON ROLE "AIRFLOW_WAREHOUSE.RAW.AIRFLOW_HFD.pii.reader.role" ;
SECURITYADMIN
transforming_warehouse.analytics.finance.pii.role


-- WILL RENAME AND REUSE THIS
SHOW GRANTS TO ROLE "AIRFLOW_WAREHOUSE.RAW.HFD.no_pii.reader.role"
SHOW GRANTS ON ROLE "AIRFLOW_WAREHOUSE.RAW.HFD.no_pii.reader.role"
