-- Create Training Dataset
CREATE OR REPLACE TABLE `retail-pipeline-beamlytics.Retail_Store.training_data` AS
SELECT * FROM `retail-pipeline-beamlytics.Retail_Store.normalized_inventory_data`
WHERE MOD(ABS(FARM_FINGERPRINT(CAST(timestamp AS STRING))), 10) < 8;

-- Create Test Dataset
CREATE OR REPLACE TABLE `retail-pipeline-beamlytics.Retail_Store.test_data` AS
SELECT * FROM `retail-pipeline-beamlytics.Retail_Store.normalized_inventory_data`
WHERE MOD(ABS(FARM_FINGERPRINT(CAST(timestamp AS STRING))), 10) >= 8;

CREATE OR REPLACE MODEL `retail-pipeline-beamlytics.Retail_Store.demand_forecasting_model`
OPTIONS(model_type='linear_reg') AS
SELECT
  normalized_count AS label,
  timestamp,
  normalized_price,
  store_id,
  departmentId,
  product_id,
  sku,
  aisleId,
  product_name_encoded,
  recipeId_encoded,
  image_encoded
FROM
  `retail-pipeline-beamlytics.Retail_Store.training_data`;

