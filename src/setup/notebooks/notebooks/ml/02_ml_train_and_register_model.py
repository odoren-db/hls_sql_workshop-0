# Databricks notebook source
# MAGIC %md
# MAGIC This notebook trains a regression model to predict claims amount.
# MAGIC
# MAGIC This notebook is designed to be able to run on serverless compute or classic compute with DBR runtime or DBR ML runtime.

# COMMAND ----------

# MAGIC %pip install mlflow

# COMMAND ----------

# MAGIC %pip install databricks-feature-engineering

# COMMAND ----------

# MAGIC %pip install databricks-feature-store

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import sklearn
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import mlflow
from mlflow.models.signature import infer_signature
from databricks.feature_store import FeatureStoreClient
from databricks.feature_engineering import FeatureLookup, FeatureEngineeringClient

# COMMAND ----------

dbutils.widgets.text('catalog','hls_sql_workshop')
catalog = dbutils.widgets.get('catalog')
print(f'catalog = {catalog}')

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ${catalog};

# COMMAND ----------

# DBTITLE 1,load data
# load training data
df = spark.table(f'{catalog}.ai.training_beneficiary')
training_data = df.select('beneficiary_code', 'claim_amount')

# Initialize Feature Store Client
fs = FeatureStoreClient()

# feature lookup
feature_lookups = [
    FeatureLookup(
      table_name=f'{catalog}.ai.feature_beneficiary',
      lookup_key='beneficiary_code'
    )]

fe = FeatureEngineeringClient()

training_set = fe.create_training_set(
  df=training_data,
  feature_lookups=feature_lookups,
  label='claim_amount',
  exclude_columns=['beneficiary_code']
)

# COMMAND ----------

# DBTITLE 1,split data and do preprocessing
from sklearn.impute import SimpleImputer
# Load the training data with features from the feature store
df_train = training_set.load_df().toPandas()

# convert flag columns to bool
columns_to_convert = [
    "deceased_flag", "esrd_flag", "heart_failure_flag", "cronic_kidney_disease_flag", "cancer_flag", 
    "copd_flag", "depression_flag", "diabetes_flag", "ischemic_heart_disease_flag", 
    "osteoporosis_flag"
]

X = df_train.drop("claim_amount", axis=1)
y = df_train["claim_amount"]    

# Split the data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Preprocessing
#date_cols = ["date_of_birth", "date_of_death"]
boolean_cols = ["deceased_flag", "esrd_flag", "heart_failure_flag", "cronic_kidney_disease_flag", "cancer_flag", "copd_flag", "depression_flag", "diabetes_flag", "ischemic_heart_disease_flag", "osteoporosis_flag"]
low_cardinality_cols = ["gender", "race", "state", "county_code"]

# Updated column groups after date preprocessing
#categorical_cols = low_cardinality_cols + boolean_cols
#all_features = date_cols + categorical_cols
all_features = low_cardinality_cols + boolean_cols

# Define imputers
boolean_imputer = SimpleImputer(strategy="constant", fill_value=0)
categorical_imputer = SimpleImputer(strategy="most_frequent")

# Pipeline for preprocessing
categorical_transformer = Pipeline(steps=[
    ("imputer", categorical_imputer),
    ("encoder", OneHotEncoder(handle_unknown="ignore", sparse=False))
])

boolean_transformer = Pipeline(steps=[
    ("imputer", boolean_imputer)
])

preprocessor = ColumnTransformer(
    transformers=[
        ("cat", categorical_transformer, low_cardinality_cols),
        ("bool", boolean_transformer, boolean_cols)
    ],
    remainder="passthrough"  # Pass date-processed columns through
)

# COMMAND ----------

# DBTITLE 1,train and log model
# Build a pipeline with a regression model
model = Pipeline(steps=[
    ("preprocessor", preprocessor),
    ("regressor", RandomForestRegressor(random_state=42))
])

# Train the model
model.fit(X_train[all_features], y_train)

# Evaluate the model
y_pred = model.predict(X_test[all_features])
mse = mean_squared_error(y_test, y_pred)
mae = mean_absolute_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print(f"Model Metrics:\n - MSE: {mse:.2f}\n - MAE: {mae:.2f}\n - R2: {r2:.2f}")

# Infer model signature
input_sample = X_test[all_features].iloc[:1]
signature = infer_signature(input_sample, model.predict(input_sample))

with mlflow.start_run(run_name='hls_sql_claims_amount') as run:
    # Log model
    fe.log_model(
        model=model,
        artifact_path="predict_claims_amount_model",
        flavor=mlflow.sklearn,
        training_set=training_set,
        signature=signature,
        registered_model_name=f'{catalog}.ai.predict_claims_amount_model'
  )

    print(f"Run ID: {mlflow.active_run().info.run_id}")

# COMMAND ----------

model_name = f'{catalog}.ai.predict_claims_amount_model'
run_id = run.info.run_id
model_uri = f"runs:/{run_id}/model"
print(model_uri)

# COMMAND ----------

# DBTITLE 1,set registered model alias
# get latest model version
model_name = f'{catalog}.ai.predict_claims_amount_model'
client = mlflow.tracking.MlflowClient()
model_version_infos = client.search_model_versions(f"name = '{model_name}'")
latest_model_version = max([model_version_info.version for model_version_info in model_version_infos])

# move the model in production
print(f"registering model version {model_name}.{latest_model_version} as production model")
client.set_registered_model_alias(model_name, "Production", latest_model_version)
