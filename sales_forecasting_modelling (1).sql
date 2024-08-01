-- Step 1: Data Cleaning and Preprocessing
CREATE OR REPLACE TABLE `retail-pipeline-beamlytics.Retail_Store.sf_cleaned_transaction_data` AS
SELECT 
    IFNULL(TIMESTAMP_SECONDS(CAST(timestamp / 1000 AS INT64)), TIMESTAMP('1970-01-01 00:00:00')) AS sale_time,
    IFNULL(uid, -1) AS uid,
    IFNULL(price, 0.0) AS price,
    IFNULL(order_number, 'Unknown') AS order_number,
    IFNULL(user_id, -1) AS user_id,
    IFNULL(store_id, -1) AS store_id,
    IFNULL(time_of_sale, 0) AS time_of_sale,
    IFNULL(department_id, -1) AS department_id,
    IFNULL(product_id, -1) AS product_id,
    IFNULL(product_count, 0) AS product_count,
    IFNULL(storeLocation.id, -1) AS store_location_id,
    IFNULL(storeLocation.state, 'Unknown') AS store_location_state,
    IFNULL(storeLocation.zip, -1) AS store_location_zip,
    IFNULL(storeLocation.city, 'Unknown') AS store_location_city,
    IFNULL(storeLocation.lat, 0.0) AS store_location_lat,
    IFNULL(storeLocation.lng, 0.0) AS store_location_lng
FROM `retail-pipeline-beamlytics.Retail_Store.clean_transaction_data`
WHERE EXTRACT(YEAR FROM TIMESTAMP_SECONDS(CAST(timestamp / 1000 AS INT64))) >= 2022;

-- Step 2: Encoding Categorical Variables
CREATE OR REPLACE TABLE `retail-pipeline-beamlytics.Retail_Store.sf_encoded_transaction_data` AS
SELECT 
    sale_time,
    uid,
    price,
    order_number,
    user_id,
    store_id,
    time_of_sale,
    department_id,
    product_id,
    product_count,
    store_location_id,
    FARM_FINGERPRINT(CAST(store_location_state AS STRING)) AS store_location_state_encoded,
    FARM_FINGERPRINT(CAST(store_location_zip AS STRING)) AS store_location_zip_encoded,
    FARM_FINGERPRINT(CAST(store_location_city AS STRING)) AS store_location_city_encoded,
    store_location_lat,
    store_location_lng
FROM `retail-pipeline-beamlytics.Retail_Store.sf_cleaned_transaction_data`;

-- Step 3: Calculate Summary Statistics
SELECT 
    MIN(price * product_count) AS min_revenue, 
    MAX(price * product_count) AS max_revenue,
    AVG(price * product_count) AS avg_revenue,
    COUNT(*) AS total_records
FROM `retail-pipeline-beamlytics.Retail_Store.sf_encoded_transaction_data`;

-- Step 4: Remove Outliers
CREATE OR REPLACE TABLE `retail-pipeline-beamlytics.Retail_Store.sf_total_transaction_data_cleaned` AS
WITH revenue_stats AS (
    SELECT 
        quantiles[OFFSET(1)] AS lower_bound,
        quantiles[OFFSET(98)] AS upper_bound
    FROM (
        SELECT 
            APPROX_QUANTILES(price * product_count, 100) AS quantiles
        FROM `retail-pipeline-beamlytics.Retail_Store.sf_encoded_transaction_data`
    )
)
SELECT * 
FROM `retail-pipeline-beamlytics.Retail_Store.sf_encoded_transaction_data`, revenue_stats
WHERE (price * product_count) >= lower_bound AND (price * product_count) <= upper_bound;

-- Step 5: Transform the Target Variable and Split Data
CREATE OR REPLACE TABLE `retail-pipeline-beamlytics.Retail_Store.sf_train_test_data` AS
SELECT 
    price,
    store_id,
    department_id,
    product_id,
    product_count,
    LOG(price * product_count) AS log_revenue,
    IF(MOD(ABS(FARM_FINGERPRINT(CAST(sale_time AS STRING))), 10) < 8, 'TRAIN', 'TEST') AS split
FROM `retail-pipeline-beamlytics.Retail_Store.sf_total_transaction_data_cleaned`;

-- Step 6: Create and Train Model
CREATE OR REPLACE MODEL `retail-pipeline-beamlytics.Retail_Store.sf_revenue_prediction_model`
OPTIONS(model_type='linear_reg', input_label_cols=['log_revenue']) AS
SELECT 
    price,
    store_id,
    department_id,
    product_id,
    product_count,
    log_revenue
FROM `retail-pipeline-beamlytics.Retail_Store.sf_train_test_data`
WHERE split = 'TRAIN';


