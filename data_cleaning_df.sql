-- Step 1: Handle Missing Values
CREATE OR REPLACE TABLE `retail-pipeline-beamlytics.Retail_Store.cleaned_inventory_data` AS
SELECT
  IFNULL(count, 0) AS count,  -- Replacing missing count with 0
  IFNULL(timestamp, 0) AS timestamp,  -- Ensuring timestamp has no missing values
  IFNULL(price, 0.0) AS price,  -- Replacing missing price with 0.0
  IFNULL(store_id, -1) AS store_id,  -- Replace missing store_id with -1
  IFNULL(departmentId, -1) AS departmentId,  -- Replace missing departmentId with -1
  IFNULL(product_id, -1) AS product_id,  -- Replace missing product_id with -1
  IFNULL(sku, -1) AS sku,  -- Replace missing sku with -1
  IFNULL(aisleId, -1) AS aisleId,  -- Replace missing aisleId with -1
  IFNULL(product_name, 'Unknown') AS product_name,  -- Replace missing product_name with 'Unknown'
  IFNULL(recipeId, 'Unknown') AS recipeId,  -- Replace missing recipeId with 'Unknown'
  IFNULL(image, 'Unknown') AS image  -- Replace missing image with 'Unknown'
FROM
  `retail-pipeline-beamlytics.Retail_Store.clean_inventory_data`;

-- Step 2: Encode Categorical Variables
--  `product_name`, `recipeId`, and `image` are categorical variables
CREATE OR REPLACE TABLE `retail-pipeline-beamlytics.Retail_Store.encoded_inventory_data` AS
SELECT
  count,
  timestamp,
  price,
  store_id,
  departmentId,
  product_id,
  sku,
  aisleId,
  product_name,
  recipeId,
  image,
  FARM_FINGERPRINT(product_name) AS product_name_encoded,  -- Encode product_name
  FARM_FINGERPRINT(recipeId) AS recipeId_encoded,  -- Encode recipeId
  FARM_FINGERPRINT(image) AS image_encoded  -- Encode image
FROM
  `retail-pipeline-beamlytics.Retail_Store.cleaned_inventory_data`;