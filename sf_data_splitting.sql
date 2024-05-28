-- Step 3: Splitting the Dataset
CREATE OR REPLACE TABLE `retail-pipeline-beamlytics.Retail_Store.transaction_data_train` AS
SELECT
  *
FROM
  `retail-pipeline-beamlytics.Retail_Store.encoded_transaction_data`
WHERE
  MOD(ABS(FARM_FINGERPRINT(CAST(timestamp AS STRING))), 10) < 8;  -- 80% for training

CREATE OR REPLACE TABLE `retail-pipeline-beamlytics.Retail_Store.transaction_data_eval` AS
SELECT
  *
FROM
  `retail-pipeline-beamlytics.Retail_Store.encoded_transaction_data`
WHERE
  MOD(ABS(FARM_FINGERPRINT(CAST(timestamp AS STRING))), 10) >= 8;  -- 20% for evaluation


