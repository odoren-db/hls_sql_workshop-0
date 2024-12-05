# Databricks notebook source
# DBTITLE 1,authenticate
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql

w = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,define name for warehouse
warehouse_name = 'SQL_Workshop_Serverless_Warehouse'

# COMMAND ----------

current_user = w.current_user.me()
user_name = current_user.user_name
print(f'User: {user_name}')

# COMMAND ----------

# ignore "already exists" error
try:
    created = w.warehouses.create(
        name=f'{warehouse_name}',
        cluster_size="Small",
        max_num_clusters=5,
        auto_stop_mins=10,
        tags=sql.EndpointTags(
            custom_tags=[sql.EndpointTagPair(key="Owner", value=f'{user_name}')
                        ])).result()
    print(f'Created serverless SQL warehouse: {warehouse_name}')
except Exception as e:
 if "already exists" in str(e):
   print(f'SQL Warehouse already exists: {warehouse_name}')
 else:
   raise e