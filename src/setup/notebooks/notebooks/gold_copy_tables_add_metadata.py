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
# MAGIC create or replace table gold.dim_date
# MAGIC as select * from gold_dim_date;
# MAGIC create or replace table gold.dim_diagnosis
# MAGIC as select * from gold_dim_diagnosis;
# MAGIC create or replace table gold.dim_provider
# MAGIC as select * from gold_dim_provider;
# MAGIC create or replace table gold.fact_carrier_claims
# MAGIC as select * from gold_fact_carrier_claims;
# MAGIC create or replace table gold.fact_patient_claims
# MAGIC as select * from gold_fact_patient_claims;
# MAGIC create or replace table gold.fact_prescription_drug_events
# MAGIC as select * from gold_fact_prescription_drug_events;
# MAGIC create or replace table gold.dim_beneficiary
# MAGIC as select * from gold_dim_beneficiary;
# MAGIC ------------------------
# MAGIC alter table gold.dim_beneficiary alter column beneficiary_key set not null;
# MAGIC alter table gold.dim_date alter column date set not null;
# MAGIC alter table gold.dim_diagnosis alter column diagnosis_key set not null;
# MAGIC alter table gold.dim_provider alter column provider_key set not null;
# MAGIC alter table gold.dim_beneficiary add constraint  beneficiary_pk primary key (beneficiary_key);
# MAGIC alter table gold.dim_date add constraint  date_pk primary key (date);
# MAGIC alter table gold.dim_diagnosis add constraint diagnosis_pk primary key (diagnosis_key);
# MAGIC alter table gold.dim_provider add constraint provider_pk primary key (provider_key);
# MAGIC alter table gold.fact_carrier_claims add constraint beneficiary_fk foreign key (beneficiary_key) references gold.dim_beneficiary;
# MAGIC alter table gold.fact_carrier_claims   add constraint claim_start_date_fk foreign key (claim_start_date) references gold.dim_date;
# MAGIC alter table gold.fact_carrier_claims   add constraint diagnosis_key_1_fk foreign key (diagnosis_key_1) references gold.dim_diagnosis;
# MAGIC alter table gold.fact_patient_claims add constraint pc_beneficiary_fk foreign key (beneficiary_key) references gold.dim_beneficiary;
# MAGIC alter table gold.fact_patient_claims   add constraint pc_claim_start_date_fk foreign key (claim_start_date) references gold.dim_date;
# MAGIC alter table gold.fact_patient_claims   add constraint pc_diagnosis_key_1_fk foreign key (diagnosis_key_1) references gold.dim_diagnosis;
# MAGIC alter table gold.fact_patient_claims   add constraint pc_attending_physician_provider_key_fk foreign key (attending_physician_provider_key) references gold.dim_provider;
# MAGIC alter table gold.fact_prescription_drug_events add constraint pde_beneficiary_fk foreign key (beneficiary_key) references gold.dim_beneficiary;
# MAGIC alter table gold.fact_prescription_drug_events   add constraint pde_rx_service_date_fk foreign key (rx_service_date) references gold.dim_date;
