CREATE DATABASE DATATUNE;
USE SCHEMA DATATUNE.PUBLIC;

-- D4B Cortex
SELECT DATE_CREATED, LANGUAGE, COUNTRY, PRODUCT, CATEGORY, DAMAGE_TYPE, TRANSCRIPT
FROM Datatune_Call_Transcripts.DASH_D4B_SCHEMA.CALL_TRANSCRIPTS
LIMIT 10;

-- Find how many languages we have to account for
-- (Snowflake Cortex supports the following for translation: https://docs.snowflake.com/LIMITEDACCESS/cortex-functions#translate )
SELECT DISTINCT LANGUAGE FROM DASH_D4B_DB.DASH_D4B_SCHEMA.CALL_TRANSCRIPTS;

-- Start with translation
SELECT 
    CASE 
        WHEN LANGUAGE = 'German' THEN SNOWFLAKE.CORTEX.TRANSLATE(transcript, 'de', 'en')
        WHEN LANGUAGE = 'French' THEN SNOWFLAKE.CORTEX.TRANSLATE(transcript, 'fr', 'en')
        ELSE TRANSCRIPT
    END as ENGLISH_TRANSCRIPT
    , *
FROM Datatune_Call_Transcripts.DASH_D4B_SCHEMA.CALL_TRANSCRIPTS
LIMIT 10;

-- Now, find sentiment
SELECT 
    SNOWFLAKE.CORTEX.SENTIMENT(transcript) as CALL_SENTIMENT
    , *
FROM Datatune_Call_Transcripts.DASH_D4B_SCHEMA.CALL_TRANSCRIPTS
LIMIT 10;

-- Finally, summarize the call
SELECT 
    SNOWFLAKE.CORTEX.SUMMARIZE(transcript) as CALL_SUMMARY
    , *
FROM Datatune_Call_Transcripts.DASH_D4B_SCHEMA.CALL_TRANSCRIPTS
LIMIT 10;

-- Combine them all into a new table
CREATE TABLE DATATUNE.PUBLIC.AUGMENTED_TRANSCRIPTS AS
SELECT
    CASE 
        WHEN LANGUAGE = 'German' THEN SNOWFLAKE.CORTEX.TRANSLATE(transcript, 'de', 'en')
        WHEN LANGUAGE = 'French' THEN SNOWFLAKE.CORTEX.TRANSLATE(transcript, 'fr', 'en')
        ELSE TRANSCRIPT
    END as ENGLISH_TRANSCRIPT
    , SNOWFLAKE.CORTEX.SENTIMENT(transcript) as CALL_SENTIMENT
    , SNOWFLAKE.CORTEX.SUMMARIZE(transcript) as CALL_SUMMARY
    , *
FROM Datatune_Call_Transcripts.DASH_D4B_SCHEMA.CALL_TRANSCRIPTS
-- LIMIT 10
;




-- TASK 2
SELECT
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(ENGLISH_TRANSCRIPT, 'What products were discussed?')
    , *
FROM TZ_SCRATCH.PUBLIC.AUGMENTED_TRANSCRIPTS
LIMIT 10;


SELECT 
    SNOWFLAKE.CORTEX.COMPLETE('llama2-70b-chat', 'What is the most important piece of gear when snow skiiing?') as Answer_70B
    , SNOWFLAKE.CORTEX.COMPLETE('llama2-7b-chat', 'What is the most important piece of gear when snow skiiing?') as Answer_7B
    ;
