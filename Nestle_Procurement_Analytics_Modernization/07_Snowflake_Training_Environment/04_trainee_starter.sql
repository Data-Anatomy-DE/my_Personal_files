-- Trainee starter. Log in with your temporary password (you will be asked to change it).
-- Your sandbox is SBX_<YOUR_NAME> with schemas RAW, STAGING, MART. Do all your work there.

SET sandbox = 'SBX_' || CURRENT_USER();
SELECT $sandbox AS my_sandbox, CURRENT_ROLE() AS my_role;

USE WAREHOUSE TRAINING_WH;
USE DATABASE IDENTIFIER($sandbox);
USE SCHEMA RAW;

-- 1. See what is available in the shared stage (read only).
LIST @TRAINING_SHARED.LANDING.RAW_STAGE;

-- 2. Create a table in YOUR sandbox, then load it from the stage.
CREATE OR REPLACE TABLE SUPPLIER_MASTER (
  SUPPLIER_ID          VARCHAR,
  SUPPLIER_NAME        VARCHAR,
  SUPPLIER_CATEGORY    VARCHAR,
  COUNTRY              VARCHAR,
  REGION               VARCHAR,
  CITY                 VARCHAR,
  PREFERRED_CURRENCY   VARCHAR,
  PAYMENT_TERMS_DAYS   NUMBER,
  TAX_ID               VARCHAR,
  SUPPLIER_STATUS      VARCHAR,
  RISK_TIER            VARCHAR,
  ONBOARDED_DATE       DATE,
  PRIMARY_CONTACT_EMAIL VARCHAR
);

COPY INTO SUPPLIER_MASTER
  FROM @TRAINING_SHARED.LANDING.RAW_STAGE/csv/supplier_master.csv
  FILE_FORMAT = (FORMAT_NAME = 'TRAINING_SHARED.LANDING.CSV_STANDARD');

SELECT * FROM SUPPLIER_MASTER LIMIT 10;

-- 3. Now repeat for the other CSVs and the JSON files (use a VARIANT column for JSON,
--    FILE_FORMAT TRAINING_SHARED.LANDING.JSON_STANDARD), then build STAGING and MART layers.
