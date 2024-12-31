# Databricks notebook source
# DBTITLE 1,Install Databricks SDK
# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

# DBTITLE 1,Python Library Restart
dbutils.library.restartPython()

# COMMAND ----------

import json

# COMMAND ----------

# DBTITLE 1,Extract Databricks SDK version
# MAGIC %pip show databricks-sdk | grep -oP '(?<=Version: )\S+'

# COMMAND ----------

# DBTITLE 1,Set Databricks Widgets
dbutils.widgets.text("catalog", "hls_sql_workshop")
dbutils.widgets.text("schema", "cms")
dbutils.widgets.text("volume", "raw_files")
dbutils.widgets.text("dlt_pipeline_id", "", "DLT Pipeline ID deployed to create the bronze, silver, gold tables")
dbutils.widgets.dropdown("compute_type", "serverless", ["serverless", "classic"])
dbutils.widgets.text("node_type_id", "m7gd.2xlarge")

# COMMAND ----------

# DBTITLE 1,Get Widget Inputs
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
dlt_pipeline_id = dbutils.widgets.get("dlt_pipeline_id")
compute_type = dbutils.widgets.get("compute_type")
node_type_id = dbutils.widgets.get("node_type_id")

# COMMAND ----------

# DBTITLE 1,File Destination Information
print(
f"""
Based on user input's the job will write files into this catalog.schema's Volume:

catalog = {catalog}
schema = {schema}
compute_type = {compute_type}

The Databricks workflow created by this notebook will write files into the following schema's Volume:
/Volumes/{catalog}/{schema}/{volume}/

Please note that is the catalog, schema, or Volume do not exist, the workflow notebooks will attempt to create them.  If the user does not have the appropriate permissions to create or use the inputted catalog, or create or use the inputted schema, the workflow will fail during execution.  Please adjust the inputted values and re-run this notebook.  
"""
)

# COMMAND ----------

# DBTITLE 1,Import Databricks Cluster Configuration Modules
from databricks.sdk.service.jobs import JobCluster
from databricks.sdk.service.compute import ClusterSpec, DataSecurityMode, RuntimeEngine

# COMMAND ----------

# DBTITLE 1,Import Databricks SDK Job and Task Configuration Modules
from databricks.sdk.service.jobs import Source, Task, NotebookTask, TaskEmailNotifications, TaskNotificationSettings, WebhookNotifications, RunIf, QueueSettings, JobParameter, JobRunAs,TaskDependency, ConditionTask, ConditionTaskOp, PipelineTask, JobCluster, PipelineParams

# COMMAND ----------

# DBTITLE 1,Databricks SDK workspace initialization
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,Get Current User
current_user = w.current_user.me()

# COMMAND ----------

# DBTITLE 1,Synthetic Data Deneration Job Inputs
current_user = w.current_user.me()
user_name = current_user.user_name
current_user_full_name = current_user.name.given_name.lower() + '_' + current_user.name.family_name.lower()

job_name = current_user_full_name + "_hls_sql_workshop"
job_cluster_key = current_user_full_name + "_hls_sql_workshop"
job_description = f"Job to generate the HLS SQL Workshop"

# COMMAND ----------

# Job cluster specification, first check if enabled for serverless
lts_version = '15.4.x-scala2.12'
if compute_type == 'serverless':
  cluster_spec = None
  job_cluster_key = None
else:
  cluster_spec = JobCluster(
    job_cluster_key = job_cluster_key
    ,new_cluster = ClusterSpec(
      spark_version = lts_version
      ,spark_conf = {
        "spark.master": "local[*, 4]",
        "spark.databricks.cluster.profile": "singleNode"
      }
      ,custom_tags = {
        "ResourceClass": "SingleNode"
      }
      ,spark_env_vars = {
        "JNAME": "zulu17-ca-amd64"
      }
      ,node_type_id = node_type_id
      ,data_security_mode = DataSecurityMode("SINGLE_USER")
      ,runtime_engine = RuntimeEngine("STANDARD")
      ,num_workers = 0
    )
  )

# COMMAND ----------

# DBTITLE 1,Print Job Inputs
print(
f"""
Current user: {current_user.user_name}

DLT Pipeline Id = {dlt_pipeline_id}

Databricks Workflow Name: {job_name}
Job cluster key: {job_cluster_key}
Job description: {job_description}

Cluster Specification Details: 
Compute_type = {compute_type}
"""
)

# COMMAND ----------

# DBTITLE 1,uc_setup
# task 0: uc_setup
uc_setup = Task(
  task_key = "uc_setup"
  ,description = "Create UC objects for hls SQL workshop"
  ,job_cluster_key = job_cluster_key
  ,notebook_task = NotebookTask(
    notebook_path = f"/Workspace/Users/{user_name}/hls_sql_workshop/src/setup/notebooks/notebooks/uc_setup"
    ,source = Source("WORKSPACE")
    ,base_parameters = dict("")
  )
  ,timeout_seconds = 0
  ,email_notifications = TaskEmailNotifications()
  ,notification_settings = TaskNotificationSettings(
    no_alert_for_skipped_runs = False
    ,no_alert_for_canceled_runs = False
    ,alert_on_last_attempt = False
  )
  ,webhook_notifications = WebhookNotifications()
)

# COMMAND ----------

# DBTITLE 1,create_sql_warehouse
# task 1: create_sql_warehouse
create_sql_warehouse = Task(
  task_key = "create_sql_warehouse"
  ,description = "Create serverless sql warehouse"
  ,depends_on = [TaskDependency(
    task_key = "uc_setup"
  )]  
  ,job_cluster_key = job_cluster_key
  ,notebook_task = NotebookTask(
    notebook_path = f"/Workspace/Users/{user_name}/hls_sql_workshop/src/setup/notebooks/notebooks/create_sql_warehouse"
    ,source = Source("WORKSPACE")
    ,base_parameters = dict("")
  )
  ,timeout_seconds = 0
  ,email_notifications = TaskEmailNotifications()
  ,notification_settings = TaskNotificationSettings(
    no_alert_for_skipped_runs = False
    ,no_alert_for_canceled_runs = False
    ,alert_on_last_attempt = False
  )
  ,webhook_notifications = WebhookNotifications()
)

# COMMAND ----------

# DBTITLE 1,Result Conditional Task
# task 2: copy_files_to_volume
copy_files_to_volume = Task(
  task_key = "copy_files_to_volume"
  ,depends_on = [TaskDependency(
    task_key = "uc_setup"
  )]
  ,run_if = RunIf("ALL_DONE")
  ,job_cluster_key = job_cluster_key  
  ,notebook_task = NotebookTask(
    notebook_path = f"/Workspace/Users/{user_name}/hls_sql_workshop/src/setup/notebooks/notebooks/copy_files_to_volume"
    ,source = Source("WORKSPACE")
    ,base_parameters = dict("")
  )
  ,timeout_seconds = 0
  ,email_notifications = TaskEmailNotifications()
  ,notification_settings = TaskNotificationSettings(
    no_alert_for_skipped_runs = False
    ,no_alert_for_canceled_runs = False
    ,alert_on_last_attempt = False
  )
  ,webhook_notifications = WebhookNotifications()
)

# COMMAND ----------

help(Task)

# COMMAND ----------

# DBTITLE 1,Unity Catalog Setup Task
# task 3: dlt_etl
dlt_etl = Task(
  task_key = "dlt_etl"
  ,depends_on = [TaskDependency(
    task_key = "copy_files_to_volume"
  )]
  ,run_if = RunIf("ALL_SUCCESS")
  ,pipeline_task = PipelineTask(
    pipeline_id = f"{dlt_pipeline_id}"
    ,full_refresh=True
  )
  ,timeout_seconds = 0
  ,email_notifications = TaskEmailNotifications()
  ,notification_settings = TaskNotificationSettings(
    no_alert_for_skipped_runs = False
    ,no_alert_for_canceled_runs = False
    ,alert_on_last_attempt = False
  )
  ,webhook_notifications = WebhookNotifications()
)

# COMMAND ----------

# DBTITLE 1,copy_gold_tables_add_metadata
# task 4: copy_gold_tables_add_metadata
copy_gold_tables_add_metadata = Task(
  task_key = "copy_gold_tables_add_metadata"
  ,depends_on = [TaskDependency(
    task_key = "dlt_etl"
  )]
  ,run_if = RunIf("ALL_SUCCESS")
  ,job_cluster_key = job_cluster_key
  ,notebook_task = NotebookTask(
    notebook_path = f"/Workspace/Users/{user_name}/hls_sql_workshop/src/setup/notebooks/notebooks/gold_copy_tables_add_metadata"
    ,source = Source("WORKSPACE")
    ,base_parameters = dict("")
  )
  ,timeout_seconds = 0
  ,email_notifications = TaskEmailNotifications()
  ,notification_settings = TaskNotificationSettings(
    no_alert_for_skipped_runs = False
    ,no_alert_for_canceled_runs = False
    ,alert_on_last_attempt = False
  )
  ,webhook_notifications = WebhookNotifications()
)

# COMMAND ----------

# DBTITLE 1,build_feature_store_beneficiary
# task 5: build_feature_store_beneficiary
build_feature_store_beneficiary = Task(
  task_key = "build_feature_store_beneficiary"
  ,depends_on = [TaskDependency(
    task_key = "copy_gold_tables_add_metadata"
  )]
  ,run_if = RunIf("ALL_SUCCESS")
  ,job_cluster_key = job_cluster_key
  ,notebook_task = NotebookTask(
    notebook_path = f"/Workspace/Users/{user_name}/hls_sql_workshop/src/setup/notebooks/notebooks/ml/01_build_training_dataset"
    ,source = Source("WORKSPACE")
    ,base_parameters = dict("")
  )
  ,timeout_seconds = 0
  ,email_notifications = TaskEmailNotifications()
  ,notification_settings = TaskNotificationSettings(
    no_alert_for_skipped_runs = False
    ,no_alert_for_canceled_runs = False
    ,alert_on_last_attempt = False
  )
  ,webhook_notifications = WebhookNotifications()
)

# COMMAND ----------

# DBTITLE 1,ml_train_and_register_model
# task 6: ml_train_and_register_model
ml_train_and_register_model = Task(
  task_key = "ml_train_and_register_model"
  ,depends_on = [TaskDependency(
    task_key = "build_feature_store_beneficiary"
  )]
  ,run_if = RunIf("ALL_SUCCESS")
  ,job_cluster_key = job_cluster_key
  ,notebook_task = NotebookTask(
    notebook_path = f"/Workspace/Users/{user_name}/hls_sql_workshop/src/setup/notebooks/notebooks/ml/02_ml_train_and_register_model"
    ,source = Source("WORKSPACE")
    ,base_parameters = dict("")
  )
  ,timeout_seconds = 0
  ,email_notifications = TaskEmailNotifications()
  ,notification_settings = TaskNotificationSettings(
    no_alert_for_skipped_runs = False
    ,no_alert_for_canceled_runs = False
    ,alert_on_last_attempt = False
  )
  ,webhook_notifications = WebhookNotifications()
)

# COMMAND ----------

# DBTITLE 1,create_online_table
# task 7: create_online_table
create_online_table = Task(
  task_key = "create_online_table"
  ,depends_on = [TaskDependency(
    task_key = "ml_train_and_register_model"
  )]
  ,run_if = RunIf("ALL_SUCCESS")
  ,job_cluster_key = job_cluster_key  
  ,notebook_task = NotebookTask(
    notebook_path = f"/Workspace/Users/{user_name}/hls_sql_workshop/src/setup/notebooks/notebooks/ml/03_create_online_table"
    ,source = Source("WORKSPACE")
    ,base_parameters = dict("")
  )
  ,timeout_seconds = 0
  ,email_notifications = TaskEmailNotifications()
  ,notification_settings = TaskNotificationSettings(
    no_alert_for_skipped_runs = False
    ,no_alert_for_canceled_runs = False
    ,alert_on_last_attempt = False
  )
  ,webhook_notifications = WebhookNotifications()
)

# COMMAND ----------

# DBTITLE 1,create_serving_endpoint
# task 8: create_serving_endpoint
create_serving_endpoint = Task(
  task_key = "create_serving_endpoint"
  ,depends_on = [TaskDependency(
    task_key = "create_online_table"
  )]
  ,run_if = RunIf("ALL_SUCCESS")
  ,job_cluster_key = job_cluster_key  
  ,notebook_task = NotebookTask(
    notebook_path = f"/Workspace/Users/{user_name}/hls_sql_workshop/src/setup/notebooks/notebooks/ml/04_create_serving_endpoint"
    ,source = Source("WORKSPACE")
    ,base_parameters = dict("")
  )
  ,timeout_seconds = 0
  ,email_notifications = TaskEmailNotifications()
  ,notification_settings = TaskNotificationSettings(
    no_alert_for_skipped_runs = False
    ,no_alert_for_canceled_runs = False
    ,alert_on_last_attempt = False
  )
  ,webhook_notifications = WebhookNotifications()
)

# COMMAND ----------

# DBTITLE 1,Existing Jobs List and Deletion
jobs_list = w.jobs.list(
  expand_tasks = False
  ,name = job_name
)
jobs_list = [job.as_dict() for job in jobs_list]
print(jobs_list, "\n")
if len(jobs_list) == 0: 
  print("No jobs found. Proceed with creating a new job...")
else:
  print("One or more jobs with the same name already exists. Deleting the jobs...\n\n")
  for i in range(0,len(jobs_list)):
    print(f"Deleting job {jobs_list[i].get('job_id')}\n")
    w.jobs.delete(jobs_list[i].get("job_id"))
  print("All jobs with the same name have been deleted. Proceed with creating a new job...")

# COMMAND ----------

# DBTITLE 1,Create Workflow
print("Attempting to create the job. Please wait...\n")

if compute_type == "classic":
  cluster_spec = [cluster_spec]

j = w.jobs.create(
  name = job_name
  ,description = job_description
  ,tasks = [
    uc_setup
    ,create_sql_warehouse
    ,copy_files_to_volume
    ,dlt_etl
    ,copy_gold_tables_add_metadata
    ,build_feature_store_beneficiary
    ,create_online_table
    ,ml_train_and_register_model 
    ,create_serving_endpoint
  ]
  ,job_clusters = cluster_spec
  ,queue = QueueSettings(enabled = True)
  ,parameters = [
    JobParameter(
      name = "catalog"
      ,default = catalog
      ,value = catalog
    )
    ,JobParameter(
      name = "schema"
      ,default = schema
      ,value = schema
    )
    ,JobParameter(
      name = "volume"
      ,default = volume
      ,value = volume
    )    
  ]
  ,run_as = JobRunAs(
    user_name = user_name
  )
  ,tags = {'project': 'hls_sql_workshop'}
)

print(f"Job created successfully. Job ID: {j.job_id}")

# COMMAND ----------

import json

dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "job": j.as_dict(),
  "job_name": job_name
}))
