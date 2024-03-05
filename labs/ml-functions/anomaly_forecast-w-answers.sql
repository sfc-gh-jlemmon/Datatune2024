USE SCHEMA FROSTBYTE_TASTY_BYTES.ANALYTICS;

-- Source/Inspiration: https://docs.snowflake.com/en/user-guide/ml-powered-forecasting#visualizing-forecasts



-- Make sure we have the right permissions set up
USE ROLE ACCOUNTADMIN;
GRANT CREATE SNOWFLAKE.ML.ANOMALY_DETECTION ON SCHEMA ANALYTICS TO ROLE TASTY_DATA_ENGINEER;
GRANT CREATE SNOWFLAKE.ML.FORECAST ON SCHEMA ANALYTICS TO ROLE TASTY_DATA_ENGINEER;
USE ROLE TASTY_DATA_ENGINEER;


-- Training View
SELECT * FROM FROSTBYTE_TASTY_BYTES.RAW_POS.ORDER_HEADER LIMIT 10;

-- TASK:  Find the range of dates present in the RAW_POS.ORDER_HEADER table
-- YOUR WORK GOES HERE --
SELECT MAX(DATE(ORDER_TS)) as MaxDate, MIN(DATE(ORDER_TS)) as MinDate
FROM FROSTBYTE_TASTY_BYTES.RAW_POS.ORDER_HEADER;
-- dates are between 1/1/2019 - 11/1/2022


















-- Let's train from Jan 2022 through Sep 2022 (the last data we have should be late 2022)
-- We will test on Oct 2022
CREATE OR REPLACE VIEW tasty_order_total_training_vw AS 
    SELECT
      TO_TIMESTAMP_NTZ(DATE(ORDER_TS)) AS ORDER_DATE,
      SUM(ORDER_AMOUNT) AS TOTAL_ORDER_AMOUNT
    FROM
      FROSTBYTE_TASTY_BYTES.RAW_POS.ORDER_HEADER
    WHERE ORDER_DATE <= '9/30/2022' AND ORDER_DATE > '1/1/2022'
    GROUP BY
      ORDER_DATE
    ORDER BY ORDER_DATE DESC;


-- Test View
-- 
--   The training view will be used to train our models, but we want to
--   test them against a smaller dataset
-- 
-- TASK:  Create a view that does not overlap the training data set to use to test our model
-- YOUR WORK GOES HERE --
CREATE OR REPLACE VIEW tasty_order_total_test_vw AS 
    SELECT
      TO_TIMESTAMP_NTZ(DATE(ORDER_TS)) AS ORDER_DATE,
      SUM(ORDER_AMOUNT) AS TOTAL_ORDER_AMOUNT
    FROM
      FROSTBYTE_TASTY_BYTES.RAW_POS.ORDER_HEADER
    WHERE ORDER_DATE >= '10/1/2022' AND ORDER_DATE < '11/1/2022' 
    GROUP BY
      ORDER_DATE
    ORDER BY ORDER_DATE DESC;







SELECT * FROM tasty_order_total_training_vw LIMIT 10;
SELECT count(*) FROM tasty_order_total_test_vw;





-- Let's start by identifying the anomalies in our data set
-- Start with a single series
CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION tasty_order_total_anomaly_model (
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'tasty_order_total_training_vw'),
  TIMESTAMP_COLNAME => 'order_date',
  TARGET_COLNAME => 'total_order_amount',
  LABEL_COLNAME => '');

-- Now, run the detection against our test view
CALL tasty_order_total_anomaly_model!DETECT_ANOMALIES(
  INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'tasty_order_total_test_vw'),
  TIMESTAMP_COLNAME => 'order_date',
  TARGET_COLNAME => 'total_order_amount'
);

-- To get this into a usable table, we will use the query cache
set anomaly_results_qid = LAST_QUERY_ID();

-- Multiply by a number to make the anomaly visible on the chart
SELECT to_number(is_anomaly) * 100000000 as AnomalyIndicator, * 
FROM TABLE(RESULT_SCAN($anomaly_results_qid))
ORDER BY TS DESC;
-- (View as a chart)


-- Next, we are going to create a forecast model.  
-- Step 1:  Create the model
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST new_tasty_order_total_forecast(
  INPUT_DATA => SYSTEM$REFERENCE('view', 'tasty_order_total_training_vw'),
  TIMESTAMP_COLNAME => 'order_date',
  TARGET_COLNAME => 'total_order_amount'
);  
-- took 15s


-- Step 2, get a forecasted value for the next 3 weeks
CALL new_tasty_order_total_forecast!FORECAST(FORECASTING_PERIODS=> 21);

-- At this point, we have the result cached!  Let's leverage the cache to see what this looks like
set forecast_qid = LAST_QUERY_ID();
SELECT * FROM TABLE(RESULT_SCAN($forecast_qid));

-- Now, we're going to combine these into a visualization to compare
SELECT ORDER_DATE, TOTAL_ORDER_AMOUNT as Actual, NULL as forecast, NULL as lower_bound, NULL as upper_bound
FROM tasty_order_total_test_vw
UNION ALL 
    SELECT ORDER_DATE, TOTAL_ORDER_AMOUNT as Actual, NULL as forecast, NULL as lower_bound, NULL as upper_bound
    FROM tasty_order_total_training_vw WHERE ORDER_DATE >= '8/1/2022'
UNION ALL
    SELECT ts as ORDER_DATE, NULL as Actual, forecast, lower_bound, upper_bound
    FROM TABLE(RESULT_SCAN($forecast_qid));

