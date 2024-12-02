
# Welcome to the HLS SQL Workshop on Databricks!

THIS DOCUMENT IS A WORK IN PRORGRESS.

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
