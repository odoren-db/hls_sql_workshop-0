-- Databricks notebook source
CREATE STREAMING LIVE TABLE bronze_beneficiary_summary
  COMMENT "raw data for summaries of beneficiaries"
AS 
SELECT 
  * 
  ,current_timestamp as load_timestamp
  ,current_timestamp as update_timestamp
  ,_metadata
FROM cloud_files("${pipeline.volume_path}/beneficiary_summary/", 'parquet')

-- COMMAND ----------

CREATE STREAMING LIVE TABLE bronze_carrier_claims
  COMMENT "raw data for carrier claim transactions"
AS 
SELECT 
  * 
  ,current_timestamp as load_timestamp
  ,current_timestamp as update_timestamp
  ,_metadata
FROM cloud_files("${pipeline.volume_path}/carrier_claims/", 'parquet')

-- COMMAND ----------

CREATE STREAMING LIVE TABLE bronze_inpatient_claims
  COMMENT "raw data for inpatient claim transactions"
AS 
SELECT 
  * 
  ,current_timestamp as load_timestamp
  ,current_timestamp as update_timestamp
  ,_metadata
FROM cloud_files("${pipeline.volume_path}/inpatient_claims/", 'parquet')

-- COMMAND ----------

CREATE STREAMING LIVE TABLE bronze_outpatient_claims
  COMMENT "raw data for outpatient claim transactions"
AS 
SELECT 
  * 
  ,current_timestamp as load_timestamp
  ,current_timestamp as update_timestamp
  ,_metadata
FROM cloud_files("${pipeline.volume_path}/outpatient_claims/", 'parquet')

-- COMMAND ----------

CREATE STREAMING LIVE TABLE bronze_prescription_drug_events
  COMMENT "raw data for prescription drug events"
AS 
SELECT 
  * 
  ,current_timestamp as load_timestamp
  ,current_timestamp as update_timestamp
  ,_metadata
FROM cloud_files("${pipeline.volume_path}/prescription_drug_events/", 'parquet')

-- COMMAND ----------

CREATE STREAMING LIVE TABLE bronze_icd_codes
  COMMENT "Lookups for icd9 codes"
AS 
SELECT
  * 
  ,current_timestamp as load_timestamp
  ,current_timestamp as update_timestamp
  ,_metadata
FROM cloud_files("${pipeline.volume_path}/icd_codes/", 'parquet')

-- COMMAND ----------

CREATE STREAMING LIVE TABLE bronze_npi_code
  COMMENT "Lookups for National Provider Identifier Number"
AS 
SELECT
  * 
  ,current_timestamp as load_timestamp
  ,current_timestamp as update_timestamp
  ,_metadata
FROM cloud_files("${pipeline.volume_path}/npi_code/", 'parquet')


-- COMMAND ----------

CREATE STREAMING LIVE TABLE bronze_lookup
  COMMENT "Code lookups accross tables"
AS 
SELECT 
  * 
  ,current_timestamp as load_timestamp
  ,current_timestamp as update_timestamp
  ,_metadata
FROM cloud_files("${pipeline.volume_path}/lookup/", 'parquet')

