# Databricks notebook source
# create widgets
dbutils.widgets.text('catalog', 'hls_sql_workshop')
dbutils.widgets.text('schema', 'cms')
dbutils.widgets.text('volume', 'raw_files')

# COMMAND ----------

# assign parameters to variables
catalog = dbutils.widgets.get(name = "catalog")
schema = dbutils.widgets.get(name = "schema")
volume = dbutils.widgets.get(name = "volume")
volume_path = f"/Volumes/{catalog}/{schema}/{volume}/"
current_user = spark.sql("SELECT current_user()").collect()[0][0]

# print values
print(f"""
  catalog = {catalog}
  schema = {schema}
  volume = {volume_path}
  current_user = {current_user}
""")

# COMMAND ----------

# DBTITLE 1,create, use, and check catalog
# MAGIC %sql
# MAGIC --create, use, and check catalog
# MAGIC create catalog if not exists ${catalog};
# MAGIC use catalog ${catalog};
# MAGIC select current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC --create gold schema
# MAGIC create schema if not exists gold;
# MAGIC
# MAGIC --create ai schema
# MAGIC create schema if not exists ai;

# COMMAND ----------

# DBTITLE 1,create, use, and check schema
# MAGIC %sql
# MAGIC --create, use, and check schema
# MAGIC create schema if not exists ${schema};
# MAGIC use schema ${schema};
# MAGIC select current_schema();

# COMMAND ----------

# MAGIC %sql
# MAGIC --create volume
# MAGIC create volume if not exists ${volume};

# COMMAND ----------

spark.sql(f'GRANT ALL PRIVILEGES ON CATALOG {catalog} TO `{current_user}`')
spark.sql(f'GRANT BROWSE ON CATALOG {catalog} TO `account users`;')
spark.sql(f'GRANT EXECUTE ON CATALOG {catalog} TO `account users`;')
spark.sql(f'GRANT READ VOLUME ON CATALOG {catalog} TO `account users`;')
spark.sql(f'GRANT SELECT ON CATALOG {catalog} TO `account users`;')
spark.sql(f'GRANT USE CATALOG ON CATALOG {catalog} TO `account users`;')
spark.sql(f'GRANT USE SCHEMA ON CATALOG {catalog} TO `account users`;')
