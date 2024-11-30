-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###dim_beneficiary

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE gold_dim_beneficiary;

APPLY CHANGES INTO
  live.gold_dim_beneficiary
FROM
  stream(live.silver_beneficiary_summary)
KEYS
  (beneficiary_code)
SEQUENCE BY
  year
COLUMNS * EXCEPT
  (year, load_timestamp,bronze_load_timestamp)
STORED AS
  SCD TYPE 2
TRACK HISTORY ON * EXCEPT 
  (year);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###dim_icd_code

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE gold_dim_diagnosis;

APPLY CHANGES INTO
  live.gold_dim_diagnosis
FROM
  stream(live.silver_icd_codes)
KEYS
  (diagnosis_key)
SEQUENCE BY
  load_timestamp
COLUMNS * EXCEPT
  (load_timestamp,bronze_load_timestamp)
STORED AS
  SCD TYPE 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###dim_provider

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE gold_dim_provider;

APPLY CHANGES INTO
  live.gold_dim_provider
FROM
  stream(live.silver_npi_codes)
KEYS
  (provider_key)
SEQUENCE BY
  load_timestamp
COLUMNS
    provider_key
   ,npi_code
   ,entity_type
   ,provider_organization_name
   ,provider_other_organization_name
   ,provider_first_line_business_mailing_address
   ,provider_second_line_business_mailing_address
   ,provider_business_mailing_address_city_name
   ,provider_business_mailing_address_state_name
   ,provider_business_mailing_address_postal_code
   ,provider_business_mailing_address_code_if_outside_us
   ,provider_first_line_business_practice_location
   ,provider_second_line_business_practice_location
   ,provider_business_practice_location_address_city_name
   ,provider_business_practice_location_address_state_name
   ,provider_business_practice_location_address_postal_code
   ,provider_business_practice_location_address_country_code_if_outside_us
STORED AS
  SCD TYPE 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###dim_date

-- COMMAND ----------

CREATE LIVE TABLE gold_dim_date
AS
SELECT * 
FROM read_files('${volume_path}/date/DimDate.csv',
  format => 'csv',
  header => true)
where year in (2008,2009,2010)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###fact_patient_claims

-- COMMAND ----------

CREATE LIVE TABLE gold_fact_patient_claims
AS
SELECT
   oc.claim_id
  ,db.beneficiary_key
  ,'Outpatient' as claim_type
  ,md5(oc.attending_physician_npi) as attending_physician_provider_key
  ,md5(oc.operating_physician_npi) as operating_physician_provider_key
  ,md5(oc.other_physician_npi) as other_physician_provider_key
  ,oc.claim_line_segment
  ,oc.claim_start_date
  ,oc.claim_end_date
  ,cast((null) as date) as inpatient_admission_date
  ,oc.claim_payment_amount
  ,oc.primary_payer_claim_paid_amount
  ,md5(oc.icd9_diagnosis_code_1) as diagnosis_key_1
  ,md5(oc.icd9_diagnosis_code_2) as diagnosis_key_2
  ,md5(oc.icd9_diagnosis_code_3) as diagnosis_key_3
  ,md5(oc.icd9_diagnosis_code_4) as diagnosis_key_4
  ,md5(oc.icd9_diagnosis_code_5) as diagnosis_key_5
  ,md5(oc.icd9_procedure_code_1) as procedure_key_1
  ,md5(oc.icd9_procedure_code_2) as procedure_key_2
  ,md5(oc.icd9_procedure_code_3) as procedure_key_3
  ,md5(oc.icd9_procedure_code_4) as procedure_key_4
  ,md5(oc.icd9_procedure_code_5) as procedure_key_5
  ,md5(oc.icd9_admitting_diagnosis_code) as admitting_key_1
FROM live.silver_outpatient_claims oc
LEFT JOIN live.gold_dim_beneficiary db on oc.beneficiary_code = db.beneficiary_code 
  AND year(oc.claim_start_date) >= db.__START_AT
  AND year(oc.claim_start_date) < coalesce(db.__END_AT,9999)

UNION SELECT
   ic.claim_id
  ,db.beneficiary_key
  ,'Inpatient' as claim_type
  ,md5(ic.attending_physician_npi) as attending_physician_provider_key
  ,md5(ic.operating_physician_npi) as operating_physician_provider_key
  ,md5(ic.other_physician_npi) as other_physician_provider_key
  ,ic.claim_line_segment
  ,ic.claim_start_date
  ,ic.claim_end_date
  ,ic.inpatient_admission_date
  ,ic.claim_payment_amount
  ,ic.primary_payer_claim_paid_amount
  ,md5(ic.icd9_diagnosis_code_1) as diagnosis_key_1
  ,md5(ic.icd9_diagnosis_code_2) as diagnosis_key_2
  ,md5(ic.icd9_diagnosis_code_3) as diagnosis_key_3
  ,md5(ic.icd9_diagnosis_code_4) as diagnosis_key_4
  ,md5(ic.icd9_diagnosis_code_5) as diagnosis_key_5
  ,md5(ic.icd9_procedure_code_1) as procedure_key_1
  ,md5(ic.icd9_procedure_code_2) as procedure_key_2
  ,md5(ic.icd9_procedure_code_3) as procedure_key_3
  ,md5(ic.icd9_procedure_code_4) as procedure_key_4
  ,md5(ic.icd9_procedure_code_5) as procedure_key_5 
  ,(null) as admitting_key_1
FROM live.silver_inpatient_claims ic
LEFT JOIN live.gold_dim_beneficiary db on ic.beneficiary_code = db.beneficiary_code 
  AND year(ic.claim_start_date) >= db.__START_AT
  AND year(ic.claim_start_date) < coalesce(db.__END_AT,9999)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###fact_carrier_claims

-- COMMAND ----------

CREATE LIVE TABLE gold_fact_carrier_claims
TBLPROPERTIES ("pipelines.autoOptimize.zOrderCols"="claim_start_date,beneficiary_key")
--PARTITIONED BY (file_name)
AS
SELECT
   md5(cc.claim_id||line_number) as claim_key
  ,cc.claim_id
  ,db.beneficiary_key
  ,cc.claim_start_date
  ,cc.claim_end_date
  ,md5(line_icd9_diagnosis_code) as line_diagnosis_key
  ,md5(claim_diagnosis_code_1) as diagnosis_key_1
  ,md5(claim_diagnosis_code_2) as diagnosis_key_2
  ,md5(claim_diagnosis_code_3) as diagnosis_key_3
  ,md5(claim_diagnosis_code_4) as diagnosis_key_4
  ,md5(claim_diagnosis_code_5) as diagnosis_key_5
  ,md5(claim_diagnosis_code_6) as diagnosis_key_6
  ,md5(claim_diagnosis_code_7) as diagnosis_key_7
  ,md5(claim_diagnosis_code_8) as diagnosis_key_8
  ,md5(provider_physician_npi_1) as provider_key_1
  ,md5(provider_physician_npi_2) as provider_key_2
  ,md5(provider_physician_npi_3) as provider_key_3
  ,md5(provider_physician_npi_4) as provider_key_4
  ,md5(provider_physician_npi_5) as provider_key_5
  ,cast(cc.claim_end_date - cc.claim_start_date as int) + 1 as claim_days
  ,cast(line_number as int) as line_number
  ,nch_payment_amount
  ,line_beneficiary_part_b_deductable_amount
  ,line_beneficiary_primary_payer_paid_amount
  ,line_coinsurance_amount
  ,line_allowed_charge_amount
  ,line_processing_indicator_code
  --,file_name
FROM live.silver_carrier_claims cc 
LEFT JOIN live.gold_dim_beneficiary db on cc.beneficiary_code = db.beneficiary_code 
  AND year(cc.claim_start_date) >= db.__START_AT
  AND year(cc.claim_start_date) < coalesce(db.__END_AT,9999)  
UNPIVOT ((nch_payment_amount,line_beneficiary_part_b_deductable_amount,line_beneficiary_primary_payer_paid_amount,line_coinsurance_amount,line_allowed_charge_amount,line_processing_indicator_code,line_icd9_diagnosis_code)
  FOR line_number in ((nch_payment_amount_1,line_beneficiary_part_b_deductable_amount_1,line_beneficiary_primary_payer_paid_amount_1,line_coinsurance_amount_1,line_allowed_charge_amount_1,line_processing_indicator_code_1,line_icd9_diagnosis_code_1) as `1`
                     ,(nch_payment_amount_2,line_beneficiary_part_b_deductable_amount_2,line_beneficiary_primary_payer_paid_amount_2,line_coinsurance_amount_2,line_allowed_charge_amount_2,line_processing_indicator_code_2,line_icd9_diagnosis_code_2) as `2`
                     ,(nch_payment_amount_3,line_beneficiary_part_b_deductable_amount_3,line_beneficiary_primary_payer_paid_amount_3,line_coinsurance_amount_3,line_allowed_charge_amount_3,line_processing_indicator_code_3,line_icd9_diagnosis_code_3) as `3`
                     ,(nch_payment_amount_4,line_beneficiary_part_b_deductable_amount_4,line_beneficiary_primary_payer_paid_amount_4,line_coinsurance_amount_4,line_allowed_charge_amount_4,line_processing_indicator_code_4,line_icd9_diagnosis_code_4) as `4`
                     ,(nch_payment_amount_5,line_beneficiary_part_b_deductable_amount_5,line_beneficiary_primary_payer_paid_amount_5,line_coinsurance_amount_5,line_allowed_charge_amount_5,line_processing_indicator_code_5,line_icd9_diagnosis_code_5) as `5`
                     ,(nch_payment_amount_6,line_beneficiary_part_b_deductable_amount_6,line_beneficiary_primary_payer_paid_amount_6,line_coinsurance_amount_6,line_allowed_charge_amount_6,line_processing_indicator_code_6,line_icd9_diagnosis_code_6) as `6`
                     ,(nch_payment_amount_7,line_beneficiary_part_b_deductable_amount_7,line_beneficiary_primary_payer_paid_amount_7,line_coinsurance_amount_7,line_allowed_charge_amount_7,line_processing_indicator_code_7,line_icd9_diagnosis_code_7) as `7`
                     ,(nch_payment_amount_8,line_beneficiary_part_b_deductable_amount_8,line_beneficiary_primary_payer_paid_amount_8,line_coinsurance_amount_8,line_allowed_charge_amount_8,line_processing_indicator_code_8,line_icd9_diagnosis_code_8) as `8`
                     ,(nch_payment_amount_9,line_beneficiary_part_b_deductable_amount_9,line_beneficiary_primary_payer_paid_amount_9,line_coinsurance_amount_9,line_allowed_charge_amount_9,line_processing_indicator_code_9,line_icd9_diagnosis_code_9) as `9`
                     ,(nch_payment_amount_10,line_beneficiary_part_b_deductable_amount_10,line_beneficiary_primary_payer_paid_amount_10,line_coinsurance_amount_10,line_allowed_charge_amount_10,line_processing_indicator_code_10,line_icd9_diagnosis_code_10) as `10`
                     ,(nch_payment_amount_11,line_beneficiary_part_b_deductable_amount_11,line_beneficiary_primary_payer_paid_amount_11,line_coinsurance_amount_11,line_allowed_charge_amount_11,line_processing_indicator_code_11,line_icd9_diagnosis_code_11) as `11`
                     ,(nch_payment_amount_12,line_beneficiary_part_b_deductable_amount_12,line_beneficiary_primary_payer_paid_amount_12,line_coinsurance_amount_12,line_allowed_charge_amount_12,line_processing_indicator_code_12,line_icd9_diagnosis_code_12) as `12`
                     ,(nch_payment_amount_13,line_beneficiary_part_b_deductable_amount_13,line_beneficiary_primary_payer_paid_amount_13,line_coinsurance_amount_13,line_allowed_charge_amount_13,line_processing_indicator_code_13,line_icd9_diagnosis_code_13) as `13`
                     ))
WHERE (nch_payment_amount <> 0 
     OR line_beneficiary_part_b_deductable_amount <> 0
     OR line_beneficiary_primary_payer_paid_amount <> 0
     OR line_coinsurance_amount <> 0
     OR line_allowed_charge_amount <> 0
     OR line_processing_indicator_code <> 0
     OR line_icd9_diagnosis_code <> 0
      )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###fact_prescription_drug_events

-- COMMAND ----------

CREATE LIVE TABLE gold_fact_prescription_drug_events
--PARTITIONED BY (file_name)
AS
SELECT
   ccw_part_d_event_number
  ,db.beneficiary_key
  ,rx_service_date
  ,product_service_id
  ,quantity_dispensed
  ,days_supply
  ,patient_pay_amount
  ,gross_drug_cost
FROM live.silver_prescription_drug_events p
LEFT JOIN live.gold_dim_beneficiary db on p.beneficiary_code = db.beneficiary_code 
  AND year(p.rx_service_date) >= db.__START_AT
  AND year(p.rx_service_date) < coalesce(db.__END_AT,9999)


-- COMMAND ----------

CREATE LIVE TABLE gold_rpt_patient_claims
AS
select
   b.*
  ,p.*
  ,c.claim_id
  ,c.claim_type
  ,c.claim_start_date
  ,c.claim_end_date
  ,c.inpatient_admission_date
  ,c.claim_payment_amount
  ,c.primary_payer_claim_paid_amount
from live.gold_fact_patient_claims c
join live.gold_dim_beneficiary b on c.beneficiary_key = b.beneficiary_key
join live.gold_dim_provider p on c.attending_physician_provider_key = p.provider_key
