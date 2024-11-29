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
# MAGIC     ,deceased_flag int
# MAGIC     ,gender string
# MAGIC     ,race string
# MAGIC     ,esrd_flag int
# MAGIC     ,state string
# MAGIC     ,county_code string
# MAGIC     ,heart_failure_flag int
# MAGIC     ,cronic_kidney_disease_flag int
# MAGIC     ,cancer_flag int
# MAGIC     ,copd_flag int
# MAGIC     ,depression_flag int
# MAGIC     ,diabetes_flag int
# MAGIC     ,ischemic_heart_disease_flag int
# MAGIC     ,osteoporosis_flag int
# MAGIC     ,asrheumatoid_arthritis_flag int
# MAGIC     ,stroke_transient_ischemic_attack_flag int
# MAGIC     ,CONSTRAINT feature_beneficiary_pk PRIMARY KEY(beneficiary_code)
# MAGIC )
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW vw_feature_beneficiary as 
# MAGIC SELECT
# MAGIC     a.beneficiary_code,
# MAGIC     CASE WHEN a.date_of_death IS NULL THEN 0 ELSE 1 END AS deceased_flag,
# MAGIC     a.gender,
# MAGIC     a.race,
# MAGIC     CASE WHEN a.esrd_flag = 'No' THEN 0 ELSE 1 END AS esrd_flag,
# MAGIC     a.state,
# MAGIC     a.county_code,
# MAGIC     CASE WHEN a.heart_failure_flag = 'No' THEN 0 ELSE 1 END AS heart_failure_flag,
# MAGIC     CASE WHEN a.cronic_kidney_disease_flag = 'No' THEN 0 ELSE 1 END AS cronic_kidney_disease_flag,
# MAGIC     CASE WHEN a.cancer_flag = 'No' THEN 0 ELSE 1 END AS cancer_flag,
# MAGIC     CASE WHEN a.copd_flag = 'No' THEN 0 ELSE 1 END AS copd_flag,
# MAGIC     CASE WHEN a.depression_flag = 'No' THEN 0 ELSE 1 END AS depression_flag,
# MAGIC     CASE WHEN a.diabetes_flag = 'No' THEN 0 ELSE 1 END AS diabetes_flag,
# MAGIC     CASE WHEN a.ischemic_heart_disease_flag = 'No' THEN 0 ELSE 1 END AS ischemic_heart_disease_flag,
# MAGIC     CASE WHEN a.osteoporosis_flag = 'No' THEN 0 ELSE 1 END AS osteoporosis_flag,
# MAGIC     CASE WHEN a.asrheumatoid_arthritis_flag = 'No' THEN 0 ELSE 1 END AS asrheumatoid_arthritis_flag,
# MAGIC     CASE WHEN a.stroke_transient_ischemic_attack_flag = 'No' THEN 0 ELSE 1 END AS stroke_transient_ischemic_attack_flag
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
