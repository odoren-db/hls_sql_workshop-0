# Databricks notebook source
# create widgets
dbutils.widgets.text('catalog', 'hls_sql_workshop')
dbutils.widgets.text('schema', 'cms')

# assign widget value to variable
catalog = dbutils.widgets.get('catalog')
schema = dbutils.widgets.get('schema')

# specify catalog to use
spark.sql(f'USE CATALOG {catalog}')
spark.sql(f'USE SCHEMA {schema}')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.dim_date AS 
# MAGIC SELECT * 
# MAGIC FROM gold_dim_date;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold.dim_diagnosis
# MAGIC  AS SELECT * FROM gold_dim_diagnosis;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold.dim_provider
# MAGIC AS SELECT * 
# MAGIC FROM gold_dim_provider;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold.fact_carrier_claims AS 
# MAGIC SELECT * 
# MAGIC FROM gold_fact_carrier_claims;
# MAGIC ALTER TABLE gold.fact_carrier_claims CLUSTER BY (beneficiary_key, claim_start_date, diagnosis_key_1);
# MAGIC OPTIMIZE gold.fact_carrier_claims;
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold.fact_patient_claims 
# MAGIC  AS 
# MAGIC SELECT * 
# MAGIC FROM gold_fact_patient_claims;
# MAGIC ALTER TABLE gold.fact_patient_claims CLUSTER BY (beneficiary_key, claim_start_date, attending_physician_provider_key);
# MAGIC OPTIMIZE gold.fact_patient_claims;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold.fact_prescription_drug_events 
# MAGIC AS 
# MAGIC SELECT * 
# MAGIC FROM gold_fact_prescription_drug_events;
# MAGIC ALTER TABLE gold.fact_prescription_drug_events   CLUSTER BY (beneficiary_key, rx_service_date);
# MAGIC OPTIMIZE gold.fact_prescription_drug_events;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold.dim_beneficiary 
# MAGIC  AS 
# MAGIC SELECT * 
# MAGIC FROM gold_dim_beneficiary;
# MAGIC ALTER TABLE gold.dim_beneficiary  CLUSTER BY (beneficiary_key);
# MAGIC OPTIMIZE gold.dim_beneficiary;
# MAGIC
# MAGIC ------------------------
# MAGIC
# MAGIC ALTER TABLE gold.dim_beneficiary 
# MAGIC ALTER COLUMN beneficiary_key SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE gold.dim_date 
# MAGIC ALTER COLUMN date SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE gold.dim_diagnosis 
# MAGIC ALTER COLUMN diagnosis_key SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE gold.dim_provider 
# MAGIC ALTER COLUMN provider_key SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE gold.dim_beneficiary 
# MAGIC ADD CONSTRAINT beneficiary_pk PRIMARY KEY (beneficiary_key);
# MAGIC
# MAGIC ALTER TABLE gold.dim_date 
# MAGIC ADD CONSTRAINT date_pk PRIMARY KEY (date);
# MAGIC
# MAGIC ALTER TABLE gold.dim_diagnosis 
# MAGIC ADD CONSTRAINT diagnosis_pk PRIMARY KEY (diagnosis_key);
# MAGIC
# MAGIC ALTER TABLE gold.dim_provider 
# MAGIC ADD CONSTRAINT provider_pk PRIMARY KEY (provider_key);
# MAGIC
# MAGIC ALTER TABLE gold.fact_carrier_claims 
# MAGIC ADD CONSTRAINT beneficiary_fk FOREIGN KEY (beneficiary_key) REFERENCES gold.dim_beneficiary;
# MAGIC
# MAGIC ALTER TABLE gold.fact_carrier_claims 
# MAGIC ADD CONSTRAINT claim_start_date_fk FOREIGN KEY (claim_start_date) REFERENCES gold.dim_date;
# MAGIC
# MAGIC ALTER TABLE gold.fact_carrier_claims 
# MAGIC ADD CONSTRAINT diagnosis_key_1_fk FOREIGN KEY (diagnosis_key_1) REFERENCES gold.dim_diagnosis;
# MAGIC
# MAGIC ALTER TABLE gold.fact_patient_claims 
# MAGIC ADD CONSTRAINT pc_beneficiary_fk FOREIGN KEY (beneficiary_key) REFERENCES gold.dim_beneficiary;
# MAGIC
# MAGIC ALTER TABLE gold.fact_patient_claims 
# MAGIC ADD CONSTRAINT pc_claim_start_date_fk FOREIGN KEY (claim_start_date) REFERENCES gold.dim_date;
# MAGIC
# MAGIC ALTER TABLE gold.fact_patient_claims 
# MAGIC ADD CONSTRAINT pc_diagnosis_key_1_fk FOREIGN KEY (diagnosis_key_1) REFERENCES gold.dim_diagnosis;
# MAGIC
# MAGIC ALTER TABLE gold.fact_patient_claims 
# MAGIC ADD CONSTRAINT pc_attending_physician_provider_key_fk FOREIGN KEY (attending_physician_provider_key) REFERENCES gold.dim_provider;
# MAGIC
# MAGIC ALTER TABLE gold.fact_prescription_drug_events 
# MAGIC ADD CONSTRAINT pde_beneficiary_fk FOREIGN KEY (beneficiary_key) REFERENCES gold.dim_beneficiary;
# MAGIC
# MAGIC ALTER TABLE gold.fact_prescription_drug_events 
# MAGIC ADD CONSTRAINT pde_rx_service_date_fk FOREIGN KEY (rx_service_date) REFERENCES gold.dim_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create report table for ai/bi dashboard
# MAGIC CREATE OR REPLACE TABLE gold.rpt_patient_claims AS
# MAGIC SELECT
# MAGIC   a.claim_type,
# MAGIC   a.claim_start_date,
# MAGIC   b.gender,
# MAGIC   b.race,
# MAGIC   b.deceased_flag,
# MAGIC   b.state,
# MAGIC   b.county_code,
# MAGIC   b.esrd_flag,
# MAGIC   b.cancer_flag,
# MAGIC   b.heart_failure_flag,
# MAGIC   b.copd_flag,
# MAGIC   b.depression_flag,
# MAGIC   b.diabetes_flag,
# MAGIC   b.ischemic_heart_disease_flag,
# MAGIC   b.osteoporosis_flag,
# MAGIC   b.rheumatoid_arthritis_flag,
# MAGIC   b.stroke_transient_ischemic_attack_flag,
# MAGIC   c.diagnosis_short_description,
# MAGIC   d.provider_organization_name,
# MAGIC   d.entity_type,
# MAGIC   sum(a.claim_payment_amount) as claim_payment_amount,
# MAGIC   sum(a.primary_payer_claim_paid_amount) as primary_payer_claim_paid_amount
# MAGIC  FROM gold.fact_patient_claims a
# MAGIC  INNER JOIN gold.dim_beneficiary b ON a.beneficiary_key = b.beneficiary_key
# MAGIC  INNER JOIN gold.dim_diagnosis c ON a.diagnosis_key_1 = c.diagnosis_key
# MAGIC  INNER JOIN gold.dim_provider d on a.attending_physician_provider_key = d.provider_key
# MAGIC  GROUP BY ALL;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION gold.get_age(start_date date) 
# MAGIC RETURNS INT
# MAGIC RETURN FLOOR(DATEDIFF('2015-12-31', start_date) / 365.25);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- add fact_patient_claims comment
# MAGIC COMMENT ON TABLE
# MAGIC gold.fact_patient_claims 
# MAGIC IS
# MAGIC "The 'fact_patient_claims' table contains information about healthcare claims, including the beneficiary, claim type, attending and operating physicians, and associated diagnoses and procedures. This data can be used to analyze healthcare costs, identify high-cost procedures, and track the performance of physicians and healthcare providers. It can also help in identifying trends in healthcare utilization and diagnoses, and in understanding the impact of different payers on healthcare costs.";
# MAGIC
# MAGIC ALTER TABLE gold.fact_patient_claims SET TAGS ('certified');
# MAGIC
# MAGIC -- add fact_carrier_claims comment
# MAGIC COMMENT ON TABLE
# MAGIC gold.fact_carrier_claims 
# MAGIC IS
# MAGIC "The 'fact_carrier_claims' table contains information about medical claims made by beneficiaries. It includes details such as the start and end dates of the claim, the diagnoses, and the providers involved. This data can be used to analyze claim patterns, diagnoses, and providers, which can help in identifying trends and potential areas for improvement in healthcare services. It can also be used to assess the effectiveness of different treatments and providers, and to identify potential fraudulent claims.";
# MAGIC
# MAGIC ALTER TABLE gold.fact_patient_claims SET TAGS ('certified');
# MAGIC
# MAGIC
# MAGIC -- add dim_diagnosis comments
# MAGIC COMMENT ON TABLE
# MAGIC gold.dim_diagnosis
# MAGIC IS
# MAGIC "The 'dim_diagnosis' table contains information about medical diagnoses. It includes details such as the diagnosis code, long and short descriptions. This data can be used to support medical diagnosis and treatment decisions, as well as for tracking and analyzing trends in medical diagnoses. It can also be used to improve the accuracy of medical diagnoses and to identify potential areas for further research or improvement in medical diagnosis and treatment.";
# MAGIC
# MAGIC ALTER TABLE gold.dim_diagnosis SET TAGS ('certified');

# COMMAND ----------

# DBTITLE 1,add column comments
# MAGIC %sql
# MAGIC ALTER TABLE gold.fact_carrier_claims ALTER COLUMN claim_key COMMENT 'Unique identifier for the claim, allowing easy reference and tracking.';
# MAGIC ALTER TABLE gold.fact_carrier_claims ALTER COLUMN claim_id COMMENT 'Identifier for the claim, allowing easy reference and tracking.';
# MAGIC ALTER TABLE gold.fact_carrier_claims ALTER COLUMN beneficiary_key COMMENT 'Identifier for the beneficiary, allowing easy reference and tracking.';
# MAGIC ALTER TABLE gold.fact_carrier_claims ALTER COLUMN claim_start_date COMMENT 'The start date of the claim, indicating when the claim period begins.';
# MAGIC ALTER TABLE gold.fact_carrier_claims ALTER COLUMN claim_end_date COMMENT 'The end date of the claim, indicating when the claim period ends.';
# MAGIC ALTER TABLE gold.fact_carrier_claims ALTER COLUMN line_diagnosis_key COMMENT 'Identifier for the main diagnosis, allowing easy reference and tracking.';
# MAGIC ALTER TABLE gold.fact_carrier_claims ALTER COLUMN diagnosis_key_1 COMMENT 'Identifier for the first additional diagnosis, allowing easy reference and tracking.';
# MAGIC ALTER TABLE gold.fact_carrier_claims ALTER COLUMN diagnosis_key_2 COMMENT 'Identifier for the second additional diagnosis, allowing easy reference and tracking.';
# MAGIC ALTER TABLE gold.fact_carrier_claims ALTER COLUMN diagnosis_key_3 COMMENT 'Identifier for the third additional diagnosis, allowing easy reference and tracking.';
# MAGIC ALTER TABLE gold.fact_carrier_claims ALTER COLUMN diagnosis_key_4 COMMENT 'Identifier for the fourth additional diagnosis, allowing easy reference and tracking.';
# MAGIC ALTER TABLE gold.fact_carrier_claims ALTER COLUMN diagnosis_key_5 COMMENT 'Identifier for the fifth additional diagnosis, allowing easy reference and tracking.';
# MAGIC ALTER TABLE gold.fact_carrier_claims ALTER COLUMN diagnosis_key_6 COMMENT 'Identifier for the sixth additional diagnosis, allowing easy reference and tracking.';
# MAGIC ALTER TABLE gold.fact_carrier_claims ALTER COLUMN diagnosis_key_7 COMMENT 'Identifier for the seventh additional diagnosis, allowing easy reference and tracking.';
# MAGIC ALTER TABLE gold.fact_carrier_claims ALTER COLUMN diagnosis_key_8 COMMENT 'Identifier for the eighth additional diagnosis, allowing easy reference and tracking.';
# MAGIC ALTER TABLE gold.fact_carrier_claims ALTER COLUMN provider_key_1 COMMENT 'Identifier for the first provider, allowing easy reference and tracking.';
# MAGIC ALTER TABLE gold.fact_carrier_claims ALTER COLUMN provider_key_2 COMMENT 'Identifier for the second provider, allowing easy reference and tracking.';
# MAGIC ALTER TABLE gold.fact_carrier_claims ALTER COLUMN provider_key_3 COMMENT 'Identifier for the third provider, allowing easy reference and tracking.';
# MAGIC ALTER TABLE gold.fact_carrier_claims ALTER COLUMN provider_key_4 COMMENT 'Identifier for the fourth provider, allowing easy reference and tracking.';
# MAGIC ALTER TABLE gold.fact_carrier_claims ALTER COLUMN provider_key_5 COMMENT 'Identifier for the fifth provider, allowing easy reference and tracking.';
# MAGIC ALTER TABLE gold.fact_carrier_claims ALTER COLUMN claim_days COMMENT 'The number of days the claim is active, indicating the duration of the claim period.';
# MAGIC ALTER TABLE gold.fact_carrier_claims ALTER COLUMN line_number COMMENT 'Unique identifier for each claim line, allowing easy reference and tracking.';
# MAGIC ALTER TABLE gold.fact_carrier_claims ALTER COLUMN nch_payment_amount COMMENT 'Represents the amount paid by the non-contracted health system (NCHS) for the claim.';
# MAGIC ALTER TABLE gold.fact_carrier_claims ALTER COLUMN line_beneficiary_part_b_deductable_amount COMMENT 'Represents the amount the beneficiary is responsible for paying after the deductible is met.';
# MAGIC ALTER TABLE gold.fact_carrier_claims ALTER COLUMN line_beneficiary_primary_payer_paid_amount COMMENT 'Represents the amount paid by the primary payer for the claim.';
# MAGIC ALTER TABLE gold.fact_carrier_claims ALTER COLUMN line_coinsurance_amount COMMENT 'Represents the amount the beneficiary is responsible for paying after the primary payer has paid their portion.';
# MAGIC ALTER TABLE gold.fact_carrier_claims ALTER COLUMN line_allowed_charge_amount COMMENT 'Represents the allowed charge for the claim, which is the maximum amount the payer will reimburse.';
# MAGIC ALTER TABLE gold.fact_carrier_claims ALTER COLUMN line_processing_indicator_code COMMENT 'Represents the processing indicator code for the claim, which can be used to identify and categorize claims.';
# MAGIC ALTER TABLE gold.fact_patient_claims ALTER COLUMN claim_id COMMENT 'Unique identifier for each claim, allowing easy reference and tracking.';
# MAGIC ALTER TABLE gold.fact_patient_claims ALTER COLUMN beneficiary_key COMMENT 'Identifier for the patient, allowing tracking of claims for individual patients.';
# MAGIC ALTER TABLE gold.fact_patient_claims ALTER COLUMN beneficiary_code COMMENT 'Code associated with the patient, providing a human-readable label for identification.';
# MAGIC ALTER TABLE gold.fact_patient_claims ALTER COLUMN claim_type COMMENT 'Represents the type of claim, such as inpatient, outpatient, or emergency.';
# MAGIC ALTER TABLE gold.fact_patient_claims ALTER COLUMN attending_physician_provider_key COMMENT 'Identifier for the attending physician, allowing tracking of claims for individual physicians.';
# MAGIC ALTER TABLE gold.fact_patient_claims ALTER COLUMN operating_physician_provider_key COMMENT 'Identifier for the operating physician, allowing tracking of claims for individual physicians.';
# MAGIC ALTER TABLE gold.fact_patient_claims ALTER COLUMN other_physician_provider_key COMMENT 'Identifier for any other physicians involved in the claim, allowing tracking of claims for individual physicians.';
# MAGIC ALTER TABLE gold.fact_patient_claims ALTER COLUMN claim_line_segment COMMENT 'Represents the line or segment of the claim, allowing tracking of individual components of a claim.';
# MAGIC ALTER TABLE gold.fact_patient_claims ALTER COLUMN claim_start_date COMMENT 'The date when the claim period begins.';
# MAGIC ALTER TABLE gold.fact_patient_claims ALTER COLUMN claim_end_date COMMENT 'The date when the claim period ends.';
# MAGIC ALTER TABLE gold.fact_patient_claims ALTER COLUMN inpatient_admission_date COMMENT 'The date when the patient was admitted to the hospital, if applicable.';
# MAGIC ALTER TABLE gold.fact_patient_claims ALTER COLUMN claim_payment_amount COMMENT 'Represents the total amount paid for the claim.';
# MAGIC ALTER TABLE gold.fact_patient_claims ALTER COLUMN primary_payer_claim_paid_amount COMMENT 'Represents the amount paid by the primary payer for the claim.';
# MAGIC ALTER TABLE gold.fact_patient_claims ALTER COLUMN diagnosis_key_1 COMMENT 'Identifier for the first diagnosis associated with the claim.';
# MAGIC ALTER TABLE gold.fact_patient_claims ALTER COLUMN diagnosis_key_2 COMMENT 'Identifier for the second diagnosis associated with the claim.';
# MAGIC ALTER TABLE gold.fact_patient_claims ALTER COLUMN diagnosis_key_3 COMMENT 'Identifier for the third diagnosis associated with the claim.';
# MAGIC ALTER TABLE gold.fact_patient_claims ALTER COLUMN diagnosis_key_4 COMMENT 'Identifier for the fourth diagnosis associated with the claim.';
# MAGIC ALTER TABLE gold.fact_patient_claims ALTER COLUMN diagnosis_key_5 COMMENT 'Identifier for the fifth diagnosis associated with the claim.';
# MAGIC ALTER TABLE gold.fact_patient_claims ALTER COLUMN procedure_key_1 COMMENT 'Identifier for the first procedure associated with the claim.';
# MAGIC ALTER TABLE gold.fact_patient_claims ALTER COLUMN procedure_key_2 COMMENT 'Identifier for the second procedure associated with the claim.';
# MAGIC ALTER TABLE gold.fact_patient_claims ALTER COLUMN procedure_key_3 COMMENT 'Unique identifier for the medical procedure, allowing for easy tracking and reference.';
# MAGIC ALTER TABLE gold.fact_patient_claims ALTER COLUMN procedure_key_4 COMMENT 'Additional unique identifier for the medical procedure, providing further distinction and organization.';
# MAGIC ALTER TABLE gold.fact_patient_claims ALTER COLUMN procedure_key_5 COMMENT 'Another unique identifier for the medical procedure, ensuring proper categorization and distinction.';
# MAGIC ALTER TABLE gold.fact_patient_claims ALTER COLUMN admitting_key_1 COMMENT 'Identifier for the patients admission, linking the medical procedure to the patients hospital stay.';
# MAGIC ALTER TABLE gold.dim_diagnosis ALTER COLUMN diagnosis_key COMMENT 'Unique identifier for each diagnosis, allowing easy reference and distinction between different diagnoses.';
# MAGIC ALTER TABLE gold.dim_diagnosis ALTER COLUMN diagnosis_code COMMENT 'A short, alphanumeric code used to represent the diagnosis in a standardized manner.';
# MAGIC ALTER TABLE gold.dim_diagnosis ALTER COLUMN diagnosis_long_description COMMENT 'A detailed description of the diagnosis, providing context and additional information.';
# MAGIC ALTER TABLE gold.dim_diagnosis ALTER COLUMN diagnosis_short_description COMMENT 'A concise summary of the diagnosis, allowing quick and easy understanding of the condition.';
