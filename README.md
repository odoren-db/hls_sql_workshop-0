## THIS DOCUMENT IS A WORK IN PRORGRESS. ##

# HLS SQL Workshop on Databricks

The HLS SQL Workshop on Databricks is an implementation of specifications derived from the TPC-DI Benchmark.
This repo includes multiple implementations and interpretations of the TPC-DI v1.1.0. We suggest executing any of the workflow types on the Databricks Runtime 14.3 or higher.

We suggest executing any of the workflow types on using Serverless compute. If Serverless compute is not available, we suggest using the Databricks Runtime **14.3 LTS** or higher. 

## Summary

This guide explores how Databricks' Data Intelligence Platform transforms modern data operations with its powerful Data Warehousing capabilities. It demonstrates ingesting claims data from CMS using Databricks Workflows, highlighting the platform’s scalability, flexibility, and cost efficiency for large-scale data processing.

Key features include:
  - Databricks SQL: Databricks SQL is the intelligent data warehouse. Built with DatabricksIQ, the Data Intelligence Engine that understands the uniqueness of your data, Databricks SQL democratizes analytics for technical and business users alik
  - Unity Catalog: A unified governance solution for data and AI assets on Databricks that provides centralized access control, auditing, lineage, and data discovery capabilities across Databricks.
  - DatabricksIQ: Data Intelligence Engine that uses AI to power all parts of the Databricks Data Intelligence Platform. It uses signals across your entire Databricks environment, including Unity Catalog, dashboards, notebooks, data pipelines and documentation to create highly specialized and accurate generative AI models that understand your data, your usage patterns and your business terminology.
  - AI/BI Dashboards: Easy-to-create, shareable dashboards built on governed data to drive informed decision-making across teams.
  - Genie Feature: Natural language interaction with data, simplifying access and democratizing data usage within organizations.

The workshop showcases how Databricks enables efficient, governed, and accessible data management for enterprise-grade solutions.

## [Prerequisites](#prerequisites)
In order to properly setup and successfully run the workshop, there are several prerequisites:
- The code base is implemented to execute successfully ONLY on the Databricks platform (GCP, AZURE, OR AWS).
- Databricks E2 Deployment
- [**Workspace for the Workshop**](#workspace-for-the-workshop)
<br>There are several preferred options for running this workshop. Please work with your account team to decide which method is best for your customer
  - Run the workshop in a CloudLabs environment.
  - Run the workshop in a Databricks Express environment.
  - Run the workshop in a Customer workspace.
- Databricks Repos functionality.
- Access to GitHub via public internet access.
- Access to Serverless compute or compute using Databricks Runtime **14.3 LTS**
- Unity Catalog enabled.
  
# Workshop Setup
Follow the steps below to setup your environment for the workshop.

## Setting up Git Integration in your workspace
> ### STEP 1: Fork Github repo (optional)
In your github repository, please fork the main repo, so that you could work with the forked repo when required. Here is how you could fork the repo in Github. 

> ### STEP 2: Create a Git folder in the workspace
In your workspace, navigate to **Workspace -> Home-> Create->Git** Folder as below. 



Once you click Repo, you will be directed to the screen below, simply input the Git repository URL (example link), and click Create Repo. 


Once the repo is created, navigate to the repo **hls_sql_workshop->python_deploy** and click on the notebook **hls_sql_workshop_driver**. 



You will be navigated to the the notebook contents below: 

> ### STEP 3: Execute the notebook

Run the notebook cell to declare the widgets and assign variables, you will have a list of widgets available for you to set up depending on what type of workflows you prefer. It supports the catalog, schema, and volume that will be created, along with if you want to use classic or serverless compute within your workflow and DLT pipeline. It is recommended to use serverless compute unless your workspace requires classic compute. 



Next, run the entirety of the notebook, which will generate the necessary  DLT pipeline and workflow with the required tasks for setup.

Here is an example of results based on my settings previously. 



Once the workflow is created, you are ready to go to the generated workflow to run it and complete setup.
## Navigating Databricks Workflow
Click on the generated task link to guide you to the workflow details below. 

Alternatively, from the Databricks homepage, navigate to the  persona menu in the top left, click on Workflows and you will see a list of the workflows that were created by yourself. Click on the specific workflow will navigate you to the workflow details page. 



> ### STEP 1: Databricks Workflow

Click on Tasks in your workflow, you will see all the tasks for the workflow, feel free to click on individual tasks and see what’s the settings for the particular task. 



> ### STEP 2: Confirm Compute

Confirm that your workflow was setup with the proper compute for your workspace. As always, serverless is always strongly recommended if your workspace is enabled for serverless.



If you selected classic compute during setup, you will see the job cluster specs that also includes the node type.


## Run the Workflow
Once you have confirmed the settings, run the workflow to complete setup. The entire workflow will take ~1 hour to complete. The serving endpoint created at the end of the workflow will take some time to be available once the workflow completes.
## Confirm SQL Warehouse
Once the workflow has completed successfully, confirm that the Serverless SQL Warehouse was created. If it was not created, please use an appropriate existing SQL Warehouse or create a new one manually. The selected SQL Warehouse should be the one used during the workshop.

> ### STEP 1: Confirm SQL Warehouse

Navigate to SQL Warehouses and confirm that the serverless SQL Warehouse was created: SQL_Workshop_Serverless_Warehouse.

The workshop will attempt to create the serverless warehouse during its execution. If it is unable to create it, the task will fail but the workflow will continue to execute.

The notebook to create the SQL Warehouse will only attempt to create a serverless warehouse (it will not attempt to create a SQL Warehouse with PRO compute), and the name will be SQL_Workshop_Serverless_Warehouse. You can manually adjust the settings (e.g. size, active/max) if needed.

> ### STEP 2: Create new SQL Warehouse manually (if needed)

If the SQL Warehouse was not created automatically during setup, manually create a new one. Below are the recommended configurations:
	Name: SQL_Workshop_Serverless_Warehouse
Size: Small
Cluster Count: 
	Min: 1
	Max: 10

Please note, if you are required to use Pro instead of serverless, please adjust the min cluster count to a higher number to support seamless concurrency for users. Also, please adjust this and the max clusters accordingly based on the number of workshop participants. 

## Confirm UC Objects
Once the workflow has completed successfully, confirm that the UC objects were all setup based on your catalog and schema widget inputs.

> ### STEP 1: Confirm Catalog and Schemas
Confirm the catalog and schemas were created. You should see the following schemas in your catalog:
ai
<name of your schema> (default value is cms)
gold


 

> ### STEP 2: Confirm Tables, Volumes, and Models
In the schema you created (default value is cms) you should see 23 bronze/silver/gold tables were created and 1 volume. The volume (default value is raw_files) should show the cms files that were created in the root directory called medicare_claims.

Tables created:


CMS data located in the volume in the root directory medicare_claims:


In the gold schema you should see the star schema that was created with dim and fact tables. These tables have primary key and foreign key constraints on them, which are required for the assistant and Genie portions of the demo (DLT currently does not allow for PK and FK constraints, therefore these tables were copied from the DLT gold tables.)



In the ai schema, you should see 3 tables and a registered model:

> ### STEP 3: Confirm Serving Endpoint
Go to **Serving** and search for _predict_claims_amount_ and confirm that the serving endpoint was created. The serving endpoint may take some time until it’s Serving endpoint state it's _Serving endpoint state is ready_

## Create Genie Space
At the time of writing, Genie Rooms are not able to be created programmatically, therefore you will need to create the Genie Space manually.

> ### STEP 1: Create Genie Space

Go to **Genie -> New ** and create the Genie Space using the setting below: 
Title: CMS Genie Space
**Description:**
**Default Warehouse: ** SQL_Warehouse_Serverless_Warehouse 
This was the warehouse created by the workflow during setup. If it was not created during setup, please select the name of the warehouse you created manually in previous steps.
**Tables:** Select all tables from the gold schema
- dim_beneficiary
- dim_date
- dim_diagnosis
- dim_provider
- fact_carrier_claims
- fact_patient_claims
- fact_prescription_drug_events
**Sample Questions:**
- What is the total number of claims submitted in a given year?
- Who are the top 5 beneficiaries by claims submitted?


# Execution
Follow the instructions below to execute the deployment and complete setting up the workshop

### To setup the workshop, please follow these instructions:
  1. **FIRST** execute the first 1 cell after this initial documentation cell which will create the widgets for the setup.
  2. **SECOND** enter values for the widgets above.
      -  **CATALOG**: The name of the catalog that all objects will be created under. This will be automatically created via the workflow that is generated assuming you have the appropriate permissions to create this object. _It is recommended to leave the catalog name as the default value so that screenshots in the user instructions match what is seen in the environment._ This will be automatically created via the workflow that is generated assuming you have the appropriate permissions to create this object. This will create the volume using default storage.
      -  **SCHEMA**: The name of the schema that the DLT objects and the Volume will be created under. _It is recommended to leave the schema as the default value so that screenshots in the user instructions match what is seen in the environment_. This will be automatically created via the workflow that is generated assuming you have the appropriate permissions to create this object. This will create the schema using default storage.
      - **VOLUME**: The name of the volume that will be created and that the CMS files will copied into. _It is recommended to leave the schema as the default value so that screenshots in the user instructions match what is seen in the environment_. This will be automatically created via the workflow that is generated assuming you have the appropriate permissions to create this object.
      - **COMPUTE_TYPE**: This is very important! This is the type of compute you would like the workflow tasks and DLT pipeline to use. This should be based off what your workspace allows. It is recommended to set **compute_type == serverless** unless your workspace requires classic compute.
  3. **THIRD** execute this notebook. Once it executes successfully, it will generate a workflow that will: 
        - setup UC (e.g. catalog, schemas, etc.) 
        - copy CMS files to your volume
        - create and execute the DLT pipeline that creates the bronze/silver/gold tables
        - train and register an ML model
        - create an online table
        - create a serving endpoint
  4. **FOURTH** once this notebook finishes executing, **you will need to manually run the workflow that it generates**. The last cell output will contain all of the configuration details and a link to the workflow to execute.
  5. **FIFTH** Once your workflow executes successfully, your dataset will be ready to run the HLS SQL Workshop.

** **IMPORTANT** **

- Everytime you execute this notebook, it will **DROP** the existing DLT pipeline and create a new one. It will also drop the existing workflow and create a new one. This means that the existing  bronze/silver/gold tables created by the original DLT pipeline will need to be fully refreshed, which can take up to 1 hour.

- The contents of this workshop are not designed to be deployed multiple times in the same workspace. The names of objects created are not unique, and certain objects will not replace existing objects.
  - EXAMPLE: The serving endpoint (name: predict_claims_amount) will not be recreated/updated when running this multiple times unless it is manually deleted.

- The workflow that is generated will run a notebook called **uc_setup** that will grant the workflow owner (the user that executed this notebook) `ALL PRIVILEGES` on the volume that is created

- The workflow that is generated will run a notebook called **uc_setup** that will grant `All account users` the following permissions on the catalog that is created:
  - `BROWSE`
  - `EXECUTE`
  - `READ VOLUME`
  - `SELECT`
  - `USE CATALOG`
  - `USE SCHEMA`


If you run into any issues, please contact Dan Davis (dan.davis@databricks.com)
