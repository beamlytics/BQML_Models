CREATE OR REPLACE TABLE `retail-pipeline-beamlytics.Retail_Store.cleaned_transaction_data` AS
SELECT
  IFNULL(timestamp, 0) AS timestamp,  -- Ensuring timestamp has no missing values
  IFNULL(uid, -1) AS uid,  -- Replace missing uid with -1
  IFNULL(price, 0.0) AS price,  -- Replace missing price with 0.0
  IFNULL(order_number, 'Unknown') AS order_number,  -- Replace missing order_number with 'Unknown'
  IFNULL(user_id, -1) AS user_id,  -- Replace missing user_id with -1
  IFNULL(store_id, -1) AS store_id,  -- Replace missing store_id with -1
  IFNULL(time_of_sale, 0) AS time_of_sale,  -- Replace missing time_of_sale with 0
  IFNULL(department_id, -1) AS department_id,  -- Replace missing department_id with -1
  IFNULL(product_id, -1) AS product_id,  -- Replace missing product_id with -1
  IFNULL(product_count, 0) AS product_count,  -- Replace missing product_count with 0
  IFNULL(storeLocation, STRUCT(-1 AS id, 'Unknown' AS state, -1 AS zip, 'Unknown' AS city, 0.0 AS lat, 0.0 AS lng)) AS storeLocation  -- Replace missing storeLocation with default values
FROM
  `retail-pipeline-beamlytics.Retail_Store.clean_transaction_data`;


