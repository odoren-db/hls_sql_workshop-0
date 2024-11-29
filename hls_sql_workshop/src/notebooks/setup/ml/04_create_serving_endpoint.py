# Databricks notebook source
dbutils.widgets.text('catalog','ddavis_hls_sql')
catalog = dbutils.widgets.get('catalog')
print(f'catalog = {catalog}')

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
                    "entity_version": "1",
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
