# Databricks notebook source
dbutils.widgets.text('catalog','ddavis_hls_sql')
catalog = dbutils.widgets.get('catalog')
print(f'catalog = {catalog}')

# COMMAND ----------
%pip install mlflow
# COMMAND ----------

from mlflow.tracking import MlflowClient
model_version_infos = MlflowClient().search_model_versions(f"name = '{catalog}.ai.predict_claims_amount_model'")
latest_model_version = max([int(model_version_info.version) for model_version_info in model_version_infos])
print(f'Latest model version: {latest_model_version}')

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
                    "entity_version": f"{latest_model_version}",
                    "workload_size": "Small",
                    "scale_to_zero_enabled": True
                }
            ]
        }
    )
except Exception as e:
 if "already exists" in str(e):
   pass
 else:
   raise e
