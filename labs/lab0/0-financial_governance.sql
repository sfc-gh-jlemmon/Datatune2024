/***************************************************************************************************
 Sourced/adapted from: https://github.com/Snowflake-Labs/sf-samples/blob/main/samples/tasty_bytes/tb_zts_financial_governance.sql 

Quickstart:   Tasty Bytes - Zero to Snowflake - Financial Governance
Version:      v1
Script:       tb_zts_financial_governance.sql         
Create Date:  2023-03-17
Author:       Jacob Kranzler
Copyright(c): 2023 Snowflake Inc. All rights reserved.
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2023-03-17          Jacob Kranzler      Initial Release
***************************************************************************************************/

/*----------------------------------------------------------------------------------
 As a Tasty Bytes Snowflake Administrator we have been tasked with gaining an 
 understanding of the features Snowflake provides to help ensure proper 
 Financial Governance is in place before we begin querying and analyzing data.

 We will start by creating one in the UI, then how to do it programmatically.
 
 Let's get started by creating our first Warehouse.
----------------------------------------------------------------------------------*/

-- In the Snowsight UI, go to Admin --> Warehouses to create a warehouse
-- See instructor slides/demo for example


-- Set our Default Time Zone
use role ACCOUNTADMIN;  -- Must have ACCOUNTADMIN to change the setting.
alter account set TIMEZONE = 'America/Chicago';
use role SYSADMIN;  -- (Best practice: change role when done using ACCOUNTADMIN)






-- Section 3: Step 1 - Role and Warehouse Context
USE ROLE accountadmin;

-- Section 3: Step 2 - Creating and Configuring a Warehouse
CREATE OR REPLACE WAREHOUSE tasty_test_wh WITH
COMMENT = 'test warehouse for tasty bytes'
    WAREHOUSE_TYPE = 'standard'
    WAREHOUSE_SIZE = 'xsmall' 
    MIN_CLUSTER_COUNT = 1 
    MAX_CLUSTER_COUNT = 2 
    SCALING_POLICY = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = true
    INITIALLY_SUSPENDED = true;
    

/*----------------------------------------------------------------------------------
Quickstart Section 4 - Creating a Resource Monitor and Applying it to our Warehouse

 With a Warehouse in place, let's now leverage Snowflakes Resource Monitors to ensure
 the Warehouse has a monthly quota that will allow our admins to track it's 
 consumed credits and ensure it is suspended if it exceeds its assigned quota.
----------------------------------------------------------------------------------*/

-- Section 4: Step 1 - Creating a Resource Monitor
USE ROLE accountadmin;
CREATE OR REPLACE RESOURCE MONITOR tasty_test_rm
WITH 
    CREDIT_QUOTA = 100 -- 100 credits
    FREQUENCY = monthly -- reset the monitor monthly
    START_TIMESTAMP = immediately -- begin tracking immediately
    TRIGGERS 
        ON 75 PERCENT DO NOTIFY -- notify accountadmins at 75%
        ON 100 PERCENT DO SUSPEND -- suspend warehouse at 100 percent, let queries finish
        ON 110 PERCENT DO SUSPEND_IMMEDIATE; -- suspend warehouse and cancel all queries at 110 percent


-- Section 4: Step 2 - Applying our Resource Monitor to our Warehouse
ALTER WAREHOUSE tasty_test_wh SET RESOURCE_MONITOR = tasty_test_rm;


/*----------------------------------------------------------------------------------
Quickstart Section 5 - Protecting our Warehouse from Long Running Queries

 With monitoring in place, let's now make sure we are protecting ourselves from bad,
 long running queries ensuring timeout parameters are adjusted on the Warehouse.
----------------------------------------------------------------------------------*/

-- Section 5: Step 1 - Exploring Warehouse Statement Parameters
SHOW PARAMETERS LIKE '%statement%' IN WAREHOUSE tasty_test_wh;


-- Section 5: Step 2 - Adjusting Warehouse Statement Timeout Parameter
ALTER WAREHOUSE tasty_test_wh SET statement_timeout_in_seconds = 1800;


-- Section 5: Step 3 - Adjusting Warehouse Statement Queued Timeout Parameter
ALTER WAREHOUSE tasty_test_wh SET statement_queued_timeout_in_seconds = 600;


/*----------------------------------------------------------------------------------
Quickstart Section 6 - Protecting our Account from Long Running Queries

 These timeout parameters are also available at the Account, User and Session level.
 As we do not expect any extremely long running queries let's also adjust these 
 parameters on our Account. 
 
 Moving forward we will plan to monitor these as our Snowflake Workloads and Usage
 grow to ensure they are continuing to protect our account from unneccesary consumption
 but also not cancelling longer jobs we expect to be running.
----------------------------------------------------------------------------------*/

-- Section 6: Step 1 - Adjusting the Account Statement Timeout Parameter
-- 6 hours max time
ALTER ACCOUNT SET statement_timeout_in_seconds = 21600; 


-- Section 6: Step 2 - Adjusting the Account Statement Queued Timeout Parameter
ALTER ACCOUNT SET statement_queued_timeout_in_seconds = 3600; 


-- Section 7: Step 5 - Suspend our Warehouse
ALTER WAREHOUSE tasty_test_wh SUSPEND;
   

/**********************************************************************/
/*------               Quickstart Reset Scripts                 ------*/
/*------   These can be ran to reset your account to a state    ------*/
/*----- that will allow you to run through this Quickstart again -----*/
/**********************************************************************/

-- USE ROLE accountadmin;
-- ALTER ACCOUNT SET statement_timeout_in_seconds = default;
-- ALTER ACCOUNT SET statement_queued_timeout_in_seconds = default; 
-- DROP WAREHOUSE IF EXISTS tasty_test_wh;
-- DROP RESOURCE MONITOR IF EXISTS tasty_test_rm; 