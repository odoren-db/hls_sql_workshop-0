# Databricks notebook source
# create widgets
dbutils.widgets.text('catalog', 'ddavis_hls_sql')
dbutils.widgets.text('schema', 'cms')
dbutils.widgets.text('volume', 'raw_files')
dbutils.widgets.dropdown("compute_type", "serverless", ["serverless","classic"])

# COMMAND ----------

# assign parameters to variables
catalog = dbutils.widgets.get(name = "catalog")
schema = dbutils.widgets.get(name = "schema")
compute_type = dbutils.widgets.get(name = "compute_type")
volume = dbutils.widgets.get(name = "volume")
volume_path = f'/Volumes/{catalog}/{schema}/{volume}/medicare_claims'

# print values
print(f"""
  catalog = {catalog}
  schema = {schema}
  compute_type = {compute_type}
  volume = {volume}
  volume_path = {volume_path}
""")

# COMMAND ----------

import os
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import pipelines
from databricks.sdk.service.pipelines import PipelineCluster

w = WorkspaceClient()
current_user = w.current_user.me()
user_name = current_user.user_name
try:
    current_user_full_name = current_user.name.given_name.lower() + '_' + current_user.name.family_name.lower()
except:
    current_user_full_name = current_user.display_name.lower().split('@')[0].replace('.', '_').replace(' ', '_')
print(f'Current user full name: {current_user_full_name}')
print(f'User name: {user_name}')

# COMMAND ----------

# DBTITLE 1,set notebook paths
notebook_path = f"/Users/{user_name}/hls_sql_workshop/src/setup/notebooks/dlt/"

bronze_path = notebook_path + "01_bronze_load_tables"
silver_path = notebook_path + "02_silver_load_tables"
gold_path = notebook_path + "03_gold_load_tables"

# COMMAND ----------

# define pipeline name
pipeline_name = current_user_full_name +'_hls_sql_workshop'

# COMMAND ----------

# DBTITLE 1,define cluster specs
if compute_type == 'serverless':
  cluster_spec = None
  serverless = True
  print(compute_type)
else:
  serverless = None
  print(compute_type)
  cluster_spec = [pipelines.PipelineCluster(
                                  label="default",
                                  num_workers=4,
                                  custom_tags={
                                      "cluster_type": "default",
                                  })
  ]

# COMMAND ----------

# create pipeline function
def create_pipeline(catalog, schema):
    created = w.pipelines.create(
        continuous=False,
        name=pipeline_name,
        libraries=[pipelines.PipelineLibrary(notebook=pipelines.NotebookLibrary(path=bronze_path)),
                pipelines.PipelineLibrary(notebook=pipelines.NotebookLibrary(path=silver_path)),
                    pipelines.PipelineLibrary(notebook=pipelines.NotebookLibrary(path=gold_path))],
        clusters=cluster_spec,
        serverless=serverless,
        channel="CURRENT",
        photon=True,
        catalog=f'{catalog}',
        target=f'{schema}',
        configuration={"volume_path": f"{volume_path}"}
            )
    return created.pipeline_id

# COMMAND ----------

# check if pipeline exists

# get pipelines
all_pipelines = w.pipelines.list_pipelines(filter=f"name LIKE '{pipeline_name}'")

# filter list
pipelines_list = list(all_pipelines)
pipeline_id = ''
for pipeline in pipelines_list:
    # delete existing pipeline and create new one
    if pipeline.creator_user_name == user_name:
        pipeline_id = pipeline.pipeline_id
        print(f'Pipeline ID: {pipeline_id}')
        w.pipelines.delete(pipeline_id=pipeline_id)
        created_pipeline_id = create_pipeline(catalog=catalog, schema=schema)
        print(f'Deleted existing pipeline and created a new one. \nNew pipeline id: {created_pipeline_id}')
    else:
        pass

# if no pipeline id was found, create new pipeline    
if pipeline_id == '' or pipeline_id == None:
    created_pipeline_id = create_pipeline(catalog=catalog, schema=schema)
    print(f'Created new pipeline. \nNew pipeline id: {created_pipeline_id}')

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "DLT Pipeline ID": created_pipeline_id,
  "DLT Pipeline Name": pipeline_name,
}))
