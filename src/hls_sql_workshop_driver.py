# Databricks notebook source
# MAGIC %md
# MAGIC # Welcome to the HLS SQL Workshop on Databricks!
# MAGIC ## Please refer to the README for additional documentation
# MAGIC
# MAGIC ### To setup the workshop, please follow these instructions:
# MAGIC   1. **FIRST** execute the first 1 cell after this initial documentation cell which will create the widgets for the setup.
# MAGIC   2. **SECOND** enter values for the widgets above.
# MAGIC       -  **CATALOG**: The name of the catalog that all objects will be created under. This will be automatically created via the workflow that is generated assuming you have the appropriate permissions to create this object. _It is recommended to leave the catalog name as the default value so that screenshots in the user instructions match what is seen in the environment._ This will be automatically created via the workflow that is generated assuming you have the appropriate permissions to create this object. This will create the volume using default storage.
# MAGIC       -  **SCHEMA**: The name of the schema that the DLT objects and the Volume will be created under. _It is recommended to leave the schema as the default value so that screenshots in the user instructions match what is seen in the environment_. This will be automatically created via the workflow that is generated assuming you have the appropriate permissions to create this object. This will create the schema using default storage.
# MAGIC       - **VOLUME**: The name of the volume that will be created and that the CMS files will copied into. _It is recommended to leave the schema as the default value so that screenshots in the user instructions match what is seen in the environment_. This will be automatically created via the workflow that is generated assuming you have the appropriate permissions to create this object.
# MAGIC       - **COMPUTE_TYPE**: This is very important! This is the type of compute you would like the workflow tasks and DLT pipeline to use. This should be based off what your workspace allows. It is recommended to set **compute_type == serverless** unless your workspace requires classic compute.
# MAGIC   3. **THIRD** execute this notebook. Once it executes successfully, it will generate a workflow that will: 
# MAGIC         - setup UC (e.g. catalog, schemas, etc.) 
# MAGIC         - copy CMS files to your volume
# MAGIC         - create and execute the DLT pipeline that creates the bronze/silver/gold tables
# MAGIC         - train and register an ML model
# MAGIC         - create an online table
# MAGIC         - create a serving endpoint
# MAGIC   4. **FOURTH** once this notebook finishes executing, **you will need to manually run the workflow that it generates**. The last cell output will contain all of the configuration details and a link to the workflow to execute.
# MAGIC   5. **FIFTH** Once your workflow executes successfully, your dataset will be ready to run the HLS SQL Workshop.
# MAGIC
# MAGIC ** **IMPORTANT** **
# MAGIC
# MAGIC - Everytime you execute this notebook, it will **DROP** the existing DLT pipeline and create a new one. It will also drop the existing workflow and create a new one. This means that the existing  bronze/silver/gold tables created by the original DLT pipeline will need to be fully refreshed, which can take up to 1 hour.
# MAGIC
# MAGIC - The contents of this workshop are not designed to be deployed multiple times in the same workspace. The names of objects created are not unique, and certain objects will not replace existing objects.
# MAGIC   - EXAMPLE: The serving endpoint (name: predict_claims_amount) will not be recreated/updated when running this multiple times unless it is manually deleted.
# MAGIC
# MAGIC - The workflow that is generated will run a notebook called **uc_setup** that will grant the workflow owner (the user that executed this notebook) `ALL PRIVILEGES` on the volume that is created
# MAGIC
# MAGIC - The workflow that is generated will run a notebook called **uc_setup** that will grant `All account users` the following permissions on the catalog that is created:
# MAGIC   - `BROWSE`
# MAGIC   - `EXECUTE`
# MAGIC   - `READ VOLUME`
# MAGIC   - `SELECT`
# MAGIC   - `USE CATALOG`
# MAGIC   - `USE SCHEMA`
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

# DBTITLE 1,Config settings
config = {
  "Catalog": catalog,
  "Schema": schema,
  "Volume": volume,
  "Compute_Type": compute_type,
  "Node_Type_ID": node_type_id
}

def print_config(dict):
  print('Config Settings:')
  for key, value in dict.items():
      print(f" {key}: {value}")

print_config(config)      

# COMMAND ----------

# DBTITLE 1,Generate DLT
import json

# run generate_dlt notebook
result = dbutils.notebook.run("./setup/generate_dlt", timeout_seconds=0, arguments={"catalog": catalog, "schema": schema, "volume": volume})

# get notebook outputs
pipeline_details = json.loads(result)
pipeline_id = pipeline_details['DLT Pipeline ID']
pipeline_name = pipeline_details['DLT Pipeline Name']

# add values to config settings
config['DLT Pipeline ID'] = pipeline_id
config['DLT Pipeline Name'] = pipeline_name
print(pipeline_details)

# COMMAND ----------

# DBTITLE 1,print config and show DLT pipeline
# print config settings
print_config(config)

# show dlt pipeline link
url = f"/pipelines/{pipeline_id}"
displayHTML(f"<h1><a href={url}>Your DLT Pipeline can be found here!</a></h1>")

# COMMAND ----------

# DBTITLE 1,Generate Workflow
# run generate_workflow notebook
result = dbutils.notebook.run("./setup/generate_workflow", timeout_seconds=0, arguments={"catalog": catalog, "schema": schema, "compute_type": compute_type, "node_type_id": node_type_id, "dlt_pipeline_id": pipeline_id})

# get notebook outputs
workflow_details = json.loads(result)
workflow_name = workflow_details['job']['job_id']
workflow_id = workflow_details['job_name']

# add values to config settings
config['Workflow ID'] = pipeline_id
config['Workflow Name'] = pipeline_name
print(workflow_details)

# COMMAND ----------

# DBTITLE 1,print config and show workflow
# print config settings
print_config(config)

# show workflow link
url = f"/#job/{workflow_details['job']['job_id']}"
displayHTML(f"<h1><a href={url}>Your Workflow can be found here. You will need to execute it manually to complete setup!</a></h1>")
