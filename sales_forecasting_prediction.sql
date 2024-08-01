-- Step 8: Make Predictions for Future Sales
CREATE OR REPLACE TABLE `retail-pipeline-beamlytics.Retail_Store.sf_revenue_predictions_future_3m` AS
SELECT 
    sale_time,
    price,
    store_id,
    department_id,
    product_id,
    product_count,
    EXP(predicted_log_revenue) AS predicted_revenue
FROM ML.PREDICT(
    MODEL `retail-pipeline-beamlytics.Retail_Store.sf_revenue_prediction_model`,
    (SELECT 
        sale_time,
        price,
        store_id,
        department_id,
        product_id,
        product_count
     FROM `retail-pipeline-beamlytics.Retail_Store.sf_encoded_transaction_data`
     WHERE DATE(sale_time) >= CURRENT_DATE()
     AND DATE(sale_time) < DATE_ADD(CURRENT_DATE(), INTERVAL 3 MONTH)));

-- Step 9: Handle Negative Predictions
CREATE OR REPLACE TABLE `retail-pipeline-beamlytics.Retail_Store.sf_final_revenue_predictions_f3` AS
SELECT 
    *,
    CASE WHEN predicted_revenue < 0 THEN 0 ELSE predicted_revenue END AS final_predicted_revenue
FROM `retail-pipeline-beamlytics.Retail_Store.sf_revenue_predictions_future_3m`;

-- Step 7: Model Evaluation
CREATE OR REPLACE TABLE `retail-pipeline-beamlytics.Retail_Store.sf_model_evaluation_metrics` AS
SELECT
    '2023-08-02' AS evaluation_date,  -- Replace with the current date or dynamic date
    mean_absolute_error,
    mean_squared_error,
    mean_squared_log_error,
    median_absolute_error,
    r2_score
FROM ML.EVALUATE(MODEL `retail-pipeline-beamlytics.Retail_Store.sf_revenue_prediction_model`,
                 (SELECT 
                      price,
                      store_id,
                      department_id,
                      product_id,
                      product_count,
                      log_revenue
                  FROM `retail-pipeline-beamlytics.Retail_Store.sf_train_test_data`
                  WHERE split = 'TEST'));

-- Create Combined Table with Predictions and Actuals for 2023
CREATE OR REPLACE TABLE `retail-pipeline-beamlytics.Retail_Store.sf_revenue_predictions_combined` AS
SELECT 
    actual.sale_time AS sale_time_actual,
    predicted.sale_time AS sale_time_predicted,
    actual.price AS price_actual,
    predicted.price AS price_predicted,
    actual.product_count AS product_count_actual,
    predicted.product_count AS product_count_predicted,
    actual.price * actual.product_count AS actual_revenue,
    predicted.final_predicted_revenue AS predicted_revenue
FROM `retail-pipeline-beamlytics.Retail_Store.sf_encoded_transaction_data` AS actual
LEFT JOIN `retail-pipeline-beamlytics.Retail_Store.sf_final_revenue_predictions_f3` AS predicted
ON actual.store_id = predicted.store_id
AND actual.department_id = predicted.department_id
AND actual.product_id = predicted.product_id
AND actual.sale_time = predicted.sale_time
WHERE EXTRACT(YEAR FROM actual.sale_time) = 2023
AND EXTRACT(YEAR FROM predicted.sale_time) = 2023;
