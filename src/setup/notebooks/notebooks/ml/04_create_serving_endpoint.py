# Databricks notebook source
dbutils.widgets.text('catalog','hls_sql_workshop')
catalog = dbutils.widgets.get('catalog')
print(f'catalog = {catalog}')

# COMMAND ----------

# MAGIC %pip install mlflow

# COMMAND ----------

# DBTITLE 1,get latest model version
# from mlflow.tracking import MlflowClient
# model_version_infos = MlflowClient().search_model_versions(f"name = '{catalog}.ai.predict_claims_amount_model'")
# latest_model_version = max([int(model_version_info.version) for model_version_info in model_version_infos])
# print(f'Latest model version: {latest_model_version}')

from mlflow.tracking import MlflowClient

# Initialize the MLflow client
client = MlflowClient()

# Define the model name and alias
model_name = f'{catalog}.ai.predict_claims_amount_model'
alias = "production"

# Get the model version information using the alias
model_version_info = client.get_model_version_by_alias(model_name, alias)

# Extract the version number
model_version = model_version_info.version

print(f'Model name: {model_name} \nModel version for alias "{alias}": {model_version}')

# COMMAND ----------

from mlflow.deployments import get_deploy_client

client = get_deploy_client("databricks")

# ignore "already exists" error
try:
    endpoint = client.create_endpoint(
        name="predict_claims_amount",
        config={
            "served_entities": [
                {
                    "name": "predict_claims_amount_entity",
                    "entity_name": f"{catalog}.ai.predict_claims_amount_model",
                    "entity_version": f"{model_version}",
                    "workload_size": "Small",
                    "scale_to_zero_enabled": True
                }
            ],
            "ai_gateway": {
                "usage_tracking_config": {
                "enabled": True
                    },
                "inference_table_config": {
                "catalog_name": f"{catalog}",
                "schema_name": "ai",
                "enabled": True
                    }
            },
            "tags": [
                {
                "key": "project",
                "value": "hls_sql_workshop"
                }
        ]
        }
    )
except Exception as e:
 if "already exists" in str(e):
   pass
 else:
   raise e
