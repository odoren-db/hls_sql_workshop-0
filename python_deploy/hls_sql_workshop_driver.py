# Databricks notebook source
# MAGIC %md
# MAGIC # Welcome to the HLS SQL Workshop on Databricks!
# MAGIC ## Please refer to the README for additional documentation
# MAGIC
# MAGIC ### To setup the workshop, please follow these instructions:
# MAGIC   1. **FIRST** execute the first 1 cell after this initial documentation cell which will create the widgets for the setup.
# MAGIC   2. **SECOND** enter values for the widgets above.
# MAGIC       -  CATALOG: The name of the catalog that all objects will be created under. This will be automatically created via the workflow that is generated assuming you have the appropriate permissions to create this object.
# MAGIC       -  SCHEMA: The name of the schema that the DLT objects and the Volume will be created under. It is recommended to leave the schema and volume as their default values unless absolutely required. This will be automatically created via the workflow that is generated assuming you have the appropriate permissions to create this object.
# MAGIC       - VOLUME: The name of the volume that will be created and that the CMS files will copied into. This will be automatically created via the workflow that is generated assuming you have the appropriate permissions to create this object.
# MAGIC       - COMPUTE_TYPE: This is very important! This is the type of compute you would like the workflow tasks and DLT pipeline to use. This should be based off what your workspace allows. It is recommended to set **compute_type == serverless** unless your workspace requires classic compute.
# MAGIC   3. **THIRD** execute this notebook. Once it executes successfully, it will generate a workflow that will: 
# MAGIC         - setup UC (e.g. catalog, schemas, etc.) 
# MAGIC         - copy CMS files to your volume
# MAGIC         - create and execute the DLT pipeline that creates the bronze/silver/gold tables
# MAGIC         - train and register an ML model
# MAGIC         - create an online table
# MAGIC         - create a serving endpoint
# MAGIC   4. **FOURTH** once this notebook finishes executing, **you will need to manually run the workflow that it generates**. The last cell output will contain the workflow name, ID, and a link to the workflow. 
# MAGIC   The name of the workflow, ID, and URL can be found in the output of the last cell.
# MAGIC   5. **FIFTH** Once your workflow executes successfully, your dataset will be ready to run the HLS SQL Workshop.
# MAGIC
# MAGIC
# MAGIC
# MAGIC <img src="https://github.com/ddavisdbrx/hls_sql_workshop/blob/main/python_deploy/img/workflow_link.jpg?raw=true" width="200"/>
# MAGIC
# MAGIC
# MAGIC If you run into any issues, please contact Dan Davis (dan.davis@databricks.com)

# COMMAND ----------

# DBTITLE 1,Set Databricks Widgets
# define widgets
dbutils.widgets.text("catalog", "ddavis_hls_sql")
dbutils.widgets.text("schema", "cms")
dbutils.widgets.text("volume", "raw_files")
dbutils.widgets.dropdown("compute_type", "serverless", ["serverless", "classic"])

# COMMAND ----------

# DBTITLE 1,Get Widget Inputs
# get widget inputs
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
compute_type = dbutils.widgets.get("compute_type")

# COMMAND ----------

# DBTITLE 1,check cloud provider
# check cloud provider type
try: 
  if compute_type == "serverless":
    print(f'compute_type set to serverless, skipping step to check cloud provider')
  else:
    cloud_provider = spark.conf.get('spark.databricks.cloudProvider') # "Azure", "GCP", or "AWS"
    print(f'Cloud provider: {cloud_provider}')
except Exception as e:
  if 'is not available' in str(e):
    print('compute_type set to "classic" but you are running this notebook on a serverless cluster. It is strongly recommended to set compute_type to "serverless" to set this workshop up using all serverless compute. If this is be design, you will need to manually correct this error and manually specify "cloud_type"')
    raise e
  else:
    raise e

# COMMAND ----------

# DBTITLE 1,define default worker and driver type for cloud provider
# define default worker and driver type for cloud provider
if compute_type == "classic":
  if cloud_provider == 'AWS':
    node_type_id = "r6id.xlarge"
  elif cloud_provider == 'GCP':
    node_type_id = "n2-standard-8"
  elif cloud_provider == 'Azure':
    node_type_id = "Standard_D8ads_v5"
else:
  node_type_id = None

print(f'Node type ID: {node_type_id}')

# COMMAND ----------

# DBTITLE 1,Generate DLT
import json
result = dbutils.notebook.run("./setup/generate_dlt", timeout_seconds=0, arguments={"catalog": catalog, "schema": schema})
pipeline_details = json.loads(result)
print(pipeline_details)

# COMMAND ----------

print(f"""
      DLT Pipeline Name: {pipeline_details['DLT Pipeline Name']}
      DLT Pipeline ID: {pipeline_details['DLT Pipeline ID']}
      """)

url = f"/pipelines/{pipeline_details['DLT Pipeline ID']}"
displayHTML(f"<h1><a href={url}>Your DLT Pipeline can be found here!</a></h1>")

# COMMAND ----------

# DBTITLE 1,Generate Workflow
result = dbutils.notebook.run("./setup/generate_workflow", timeout_seconds=0, arguments={"catalog": catalog, "schema": schema, "compute_type": compute_type, "node_type_id": node_type_id, "dlt_pipeline_id": pipeline_id})
workflow_details = json.loads(result)
print(workflow_details)

# COMMAND ----------

print(f"""
      Workflow Name: {workflow_details['job']['job_id']}
      Workflow ID: {workflow_details['job_name']}
      """)

url = f"/#job/{workflow_details['job']['job_id']}"
displayHTML(f"<h1><a href={url}>Your Workflow can be found here!</a></h1>")
