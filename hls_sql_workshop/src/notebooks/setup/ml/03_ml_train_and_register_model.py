# Databricks notebook source
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

# DBTITLE 1,load data
# load training data
df = spark.table(f'ddavis_hls_sql.ai.training_beneficiary')
training_data = df.select('beneficiary_code', 'claim_amount')

# Initialize Feature Store Client
fs = FeatureStoreClient()

# feature lookup
feature_lookups = [
    FeatureLookup(
      table_name='ddavis_hls_sql.ai.feature_beneficiary',
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
    "esrd_flag", "heart_failure_flag", "cronic_kidney_disease_flag", "cancer_flag", 
    "copd_flag", "depression_flag", "diabetes_flag", "ischemic_heart_disease_flag", 
    "osteoporosis_flag", "asrheumatoid_arthritis_flag", "stroke_transient_ischemic_attack_flag"
]

# Convert "no" to 0 and "yes" to 1 for the specified columns
for col in columns_to_convert:
    df_train[col] = df_train[col].apply(lambda x: 1 if x == "Yes" else 0)


#df_train['date_of_death'] =  df_train['date_of_death'].fillna('2100-12-31')

X = df_train.drop("claim_amount", axis=1)
y = df_train["claim_amount"]    

# Split the data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Preprocessing
date_cols = ["date_of_birth", "date_of_death"]
boolean_cols = ["esrd_flag", "heart_failure_flag", "cronic_kidney_disease_flag", "cancer_flag", "copd_flag", "depression_flag", "diabetes_flag", "ischemic_heart_disease_flag", "osteoporosis_flag", "asrheumatoid_arthritis_flag", "stroke_transient_ischemic_attack_flag"]
low_cardinality_cols = ["gender", "race", "state", "county_code"]

# Date preprocessing: convert to year, month, and day features
def preprocess_dates(df, impute_strategy="constant", fill_value="2100-12-31"):
    for col in date_cols:
        # Impute missing date values
        imputer = SimpleImputer(strategy=impute_strategy, fill_value=fill_value)
        df[col] = imputer.fit_transform(df[[col]])

        # Convert to datetime and extract components
        df[f"{col}_year"] = pd.to_datetime(df[col]).dt.year
        df[f"{col}_month"] = pd.to_datetime(df[col]).dt.month
        df[f"{col}_day"] = pd.to_datetime(df[col]).dt.day
        df.drop(col, axis=1, inplace=True)
    return df

X_train = preprocess_dates(X_train)
X_test = preprocess_dates(X_test)

# Updated column groups after date preprocessing
date_processed_cols = [f"{col}_{suffix}" for col in date_cols for suffix in ["year", "month", "day"]]
categorical_cols = low_cardinality_cols + boolean_cols
all_features = date_processed_cols + categorical_cols

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

with mlflow.start_run(run_name='my_sample_run') as run:
    # Log model
    mlflow.sklearn.log_model(model, "model", signature=signature)
    
    # Log metrics
    mlflow.log_metric("mse", mse)
    mlflow.log_metric("mae", mae)
    mlflow.log_metric("r2", r2)

    # Log parameters (e.g., model type and hyperparameters)
    mlflow.log_param("model_type", "RandomForestRegressor")
    mlflow.log_param("random_state", 42)

    print(f"Run ID: {mlflow.active_run().info.run_id}")

# COMMAND ----------

# DBTITLE 1,register model
# register_model
model_name = 'ddavis_hls_sql.ai.sample_model'
run_id = run.info.run_id
model_uri = f"runs:/{run_id}/model"
model_registered = mlflow.register_model(model_uri=model_uri, name=model_name)


# move the model in production
client = mlflow.tracking.MlflowClient()
print("registering model version "+model_registered.version+" as production model")
client.set_registered_model_alias(model_name, "Production", model_registered.version)
