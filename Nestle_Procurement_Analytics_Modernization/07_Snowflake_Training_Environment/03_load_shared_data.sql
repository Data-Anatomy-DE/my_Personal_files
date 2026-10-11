-- Upload the project data into the shared internal stage.
-- PUT only works from a client (snowsql / Snowflake CLI), not from a Snowsight worksheet.
-- Run from the repo root, e.g.:
--   snowsql -a <account> -u <admin_user> -r SYSADMIN -f Nestle_Procurement_Analytics_Modernization/07_Snowflake_Training_Environment/03_load_shared_data.sql

USE ROLE SYSADMIN;
USE WAREHOUSE TRAINING_WH;
USE SCHEMA TRAINING_SHARED.LANDING;

PUT file://Nestle_Procurement_Analytics_Modernization/04_Data/csv/*.csv  @RAW_STAGE/csv/  AUTO_COMPRESS = FALSE OVERWRITE = TRUE;
PUT file://Nestle_Procurement_Analytics_Modernization/04_Data/json/*.json @RAW_STAGE/json/ AUTO_COMPRESS = FALSE OVERWRITE = TRUE;

LIST @RAW_STAGE;
