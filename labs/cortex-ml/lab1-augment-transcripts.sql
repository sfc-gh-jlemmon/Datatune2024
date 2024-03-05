-- See https://docs.snowflake.com/LIMITEDACCESS/cortex-functions for details!
USE ROLE ACCOUNTADMIN;

CREATE ROLE cortex_user_role;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE cortex_user_role;

GRANT ROLE cortex_user_role TO ROLE SYSADMIN;


CREATE DATABASE DATATUNE;
USE SCHEMA DATATUNE.PUBLIC;

-- D4B Cortex
SELECT DATE_CREATED, LANGUAGE, COUNTRY, PRODUCT, CATEGORY, DAMAGE_TYPE, TRANSCRIPT
FROM Datatune_Call_Transcripts.DASH_D4B_SCHEMA.CALL_TRANSCRIPTS
LIMIT 10;

-- Find how many languages we have to account for
-- (Snowflake Cortex supports the following for translation: https://docs.snowflake.com/LIMITEDACCESS/cortex-functions#translate )
SELECT DISTINCT LANGUAGE FROM Datatune_Call_Transcripts.DASH_D4B_SCHEMA.CALL_TRANSCRIPTS;

-- Start with translation
SELECT 
    CASE 
    END as ENGLISH_TRANSCRIPT
    , *
FROM Datatune_Call_Transcripts.DASH_D4B_SCHEMA.CALL_TRANSCRIPTS
LIMIT 10;

-- See https://docs.snowflake.com/user-guide/snowflake-cortex/llm-functions#llm-functions-overview 
-- Now, find sentiment
SELECT 

FROM Datatune_Call_Transcripts.DASH_D4B_SCHEMA.CALL_TRANSCRIPTS
LIMIT 10;

-- Finally, summarize the call
SELECT 

FROM Datatune_Call_Transcripts.DASH_D4B_SCHEMA.CALL_TRANSCRIPTS
LIMIT 10;

-- Combine them all into a new table
CREATE TABLE DATATUNE.PUBLIC.AUGMENTED_TRANSCRIPTS AS

;
