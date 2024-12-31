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
volume_path = f"/Volumes/{catalog}/{schema}/{volume}/medicare_claims"

# print values
print(f"""
  catalog = {catalog}
  schema = {schema}
  volume = {volume_path}
""")

# COMMAND ----------

# define source location of data, which is blob storage
blob = "wasbs://cmsdata@hlssqlworkshopsa.blob.core.windows.net/"

# define folders to check if exist
folders_to_check = ['beneficiary_summary', 'carrier_claims', 'date', 'icd_codes', 'inpatient_claims', 'lookup', 'npi_code', 'outpatient_claims', 'prescription_drug_events']

# check if folders already exist in volume. If not, copy the data over
for folder in folders_to_check:
  target = f"{volume_path}/{folder}"
  try:
    print(f'Checking if folder exists in volume. Folder: {folder}')
    dbutils.fs.ls(f"{target}")
    print(f'  Folder already exists in volume. Skipping copy.')
  except:
    source = f'{blob}{folder}/'
    print(f'Path does not exist. Copying files from source to target \n source: {source} \n target: {target}')
    dbutils.fs.cp(source, target, True)
