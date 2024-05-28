SELECT
  *
FROM
  ML.EVALUATE(MODEL `retail-pipeline-beamlytics.Retail_Store.demand_forecasting_model`,
              (
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
                  `retail-pipeline-beamlytics.Retail_Store.test_data`
              ));

SELECT
  *
FROM
  ML.PREDICT(MODEL `retail-pipeline-beamlytics.Retail_Store.demand_forecasting_model`,
             (
               SELECT
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
                 `retail-pipeline-beamlytics.Retail_Store.normalized_inventory_data`
             ));
