-- Step 3: Normalize Numerical Features
-- Using standard scaling: (value - mean) / stddev
CREATE OR REPLACE TABLE `retail-pipeline-beamlytics.Retail_Store.normalized_inventory_data` AS
WITH stats AS (
  SELECT
    AVG(count) AS mean_count,
    STDDEV(count) AS stddev_count,
    AVG(price) AS mean_price,
    STDDEV(price) AS stddev_price
  FROM
    `retail-pipeline-beamlytics.Retail_Store.encoded_inventory_data`
)
SELECT
  (count - stats.mean_count) / stats.stddev_count AS normalized_count,
  timestamp,
  (price - stats.mean_price) / stats.stddev_price AS normalized_price,
  store_id,
  departmentId,
  product_id,
  sku,
  aisleId,
  product_name_encoded,
  recipeId_encoded,
  image_encoded
FROM
  `retail-pipeline-beamlytics.Retail_Store.encoded_inventory_data`, stats;
