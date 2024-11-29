# Databricks notebook source
dbutils.widgets.text('catalog','ddavis_hls_sql')
catalog = dbutils.widgets.get('catalog')
print(f'catalog = {catalog}')

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ${catalog};

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ai.training_beneficiary(
# MAGIC     beneficiary_code string not null
# MAGIC     ,date_of_birth date
# MAGIC     ,date_of_death date
# MAGIC     ,gender string
# MAGIC     ,race string
# MAGIC     ,esrd_flag string
# MAGIC     ,state string
# MAGIC     ,county_code string
# MAGIC     ,heart_failure_flag string
# MAGIC     ,cronic_kidney_disease_flag string
# MAGIC     ,cancer_flag string
# MAGIC     ,copd_flag string
# MAGIC     ,depression_flag string
# MAGIC     ,diabetes_flag string
# MAGIC     ,ischemic_heart_disease_flag string
# MAGIC     ,osteoporosis_flag string
# MAGIC     ,asrheumatoid_arthritis_flag string
# MAGIC     ,stroke_transient_ischemic_attack_flag string
# MAGIC     ,claim_amount double
# MAGIC     ,CONSTRAINT training_beneficiary_pk PRIMARY KEY(beneficiary_code)
# MAGIC )
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW vw_training_beneficiary
# MAGIC as
# MAGIC SELECT
# MAGIC     a.beneficiary_code,
# MAGIC     a.date_of_birth,
# MAGIC     a.date_of_death,
# MAGIC     a.gender,
# MAGIC     a.race,
# MAGIC     a.esrd_flag,
# MAGIC     a.state,
# MAGIC     a.county_code,
# MAGIC     a.heart_failure_flag,
# MAGIC     a.cronic_kidney_disease_flag,
# MAGIC     a.cancer_flag,
# MAGIC     a.copd_flag,
# MAGIC     a.depression_flag,
# MAGIC     a.diabetes_flag,
# MAGIC     a.ischemic_heart_disease_flag,
# MAGIC     a.osteoporosis_flag,
# MAGIC     a.asrheumatoid_arthritis_flag,
# MAGIC     a.stroke_transient_ischemic_attack_flag,
# MAGIC     b.claim_amount
# MAGIC FROM
# MAGIC     cms.gold_dim_beneficiary AS a
# MAGIC INNER JOIN (
# MAGIC     SELECT
# MAGIC         d.beneficiary_code,
# MAGIC         SUM(f.claim_payment_amount) AS claim_amount
# MAGIC     FROM
# MAGIC         cms.gold_fact_patient_claims AS f
# MAGIC     JOIN
# MAGIC         cms.gold_dim_beneficiary AS d ON f.beneficiary_key = d.beneficiary_key
# MAGIC     GROUP BY
# MAGIC         d.beneficiary_code
# MAGIC     LIMIT 30000
# MAGIC ) AS b ON b.beneficiary_code = a.beneficiary_code
# MAGIC WHERE
# MAGIC     a.__END_AT IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO ai.training_beneficiary
# MAGIC USING vw_training_beneficiary
# MAGIC   ON vw_training_beneficiary.beneficiary_code = ai.training_beneficiary.beneficiary_code
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ai.training_beneficiary
# MAGIC limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ai.feature_beneficiary(
# MAGIC     beneficiary_code string not null
# MAGIC     ,date_of_birth date
# MAGIC     ,date_of_death date
# MAGIC     ,gender string
# MAGIC     ,race string
# MAGIC     ,esrd_flag string
# MAGIC     ,state string
# MAGIC     ,county_code string
# MAGIC     ,heart_failure_flag string
# MAGIC     ,cronic_kidney_disease_flag string
# MAGIC     ,cancer_flag string
# MAGIC     ,copd_flag string
# MAGIC     ,depression_flag string
# MAGIC     ,diabetes_flag string
# MAGIC     ,ischemic_heart_disease_flag string
# MAGIC     ,osteoporosis_flag string
# MAGIC     ,asrheumatoid_arthritis_flag string
# MAGIC     ,stroke_transient_ischemic_attack_flag string
# MAGIC     ,CONSTRAINT feature_beneficiary_pk PRIMARY KEY(beneficiary_code)
# MAGIC )
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW vw_feature_beneficiary as 
# MAGIC SELECT
# MAGIC     a.beneficiary_code,
# MAGIC     a.date_of_birth,
# MAGIC     a.date_of_death,
# MAGIC     a.gender,
# MAGIC     a.race,
# MAGIC     a.esrd_flag,
# MAGIC     a.state,
# MAGIC     a.county_code,
# MAGIC     a.heart_failure_flag,
# MAGIC     a.cronic_kidney_disease_flag,
# MAGIC     a.cancer_flag,
# MAGIC     a.copd_flag,
# MAGIC     a.depression_flag,
# MAGIC     a.diabetes_flag,
# MAGIC     a.ischemic_heart_disease_flag,
# MAGIC     a.osteoporosis_flag,
# MAGIC     a.asrheumatoid_arthritis_flag,
# MAGIC     a.stroke_transient_ischemic_attack_flag,
# MAGIC     b.claim_amount
# MAGIC FROM
# MAGIC     cms.gold_dim_beneficiary AS a
# MAGIC INNER JOIN (
# MAGIC     SELECT
# MAGIC         d.beneficiary_code,
# MAGIC         SUM(f.claim_payment_amount) AS claim_amount
# MAGIC     FROM
# MAGIC         cms.gold_fact_patient_claims AS f
# MAGIC     JOIN
# MAGIC         cms.gold_dim_beneficiary AS d ON f.beneficiary_key = d.beneficiary_key
# MAGIC     GROUP BY
# MAGIC         d.beneficiary_code
# MAGIC ) AS b ON b.beneficiary_code = a.beneficiary_code
# MAGIC WHERE
# MAGIC     a.__END_AT IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO ai.feature_beneficiary
# MAGIC USING vw_feature_beneficiary
# MAGIC   ON vw_feature_beneficiary.beneficiary_code = ai.feature_beneficiary.beneficiary_code
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
