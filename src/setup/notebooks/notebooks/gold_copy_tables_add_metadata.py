# Databricks notebook source
# create widgets
dbutils.widgets.text('catalog', 'ddavis_hls_sql')
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
# MAGIC CREATE OR REPLACE FUNCTION gold.get_age(start_date date) 
# MAGIC RETURNS INT
# MAGIC RETURN FLOOR(DATEDIFF('2015-12-31', start_date) / 365.25);

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE
# MAGIC gold.fact_patient_claims 
# MAGIC IS
# MAGIC "The 'fact_patient_claims' table contains information about healthcare claims, including the beneficiary, claim type, attending and operating physicians, and associated diagnoses and procedures. This data can be used to analyze healthcare costs, identify high-cost procedures, and track the performance of physicians and healthcare providers. It can also help in identifying trends in healthcare utilization and diagnoses, and in understanding the impact of different payers on healthcare costs.";
# MAGIC
# MAGIC ALTER TABLE gold.fact_patient_claims SET TAGS ('certified')

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE
# MAGIC gold.fact_carrier_claims 
# MAGIC IS
# MAGIC "The 'fact_carrier_claims' table contains information about medical claims made by beneficiaries. It includes details such as the start and end dates of the claim, the diagnoses, and the providers involved. This data can be used to analyze claim patterns, diagnoses, and providers, which can help in identifying trends and potential areas for improvement in healthcare services. It can also be used to assess the effectiveness of different treatments and providers, and to identify potential fraudulent claims.";
# MAGIC
# MAGIC ALTER TABLE gold.fact_patient_claims SET TAGS ('certified')
