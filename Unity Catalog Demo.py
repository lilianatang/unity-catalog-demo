# Databricks notebook source
# MAGIC %md
# MAGIC ## Unity Catalog - Your Unified Data Governance for All Data & AI Assets
# MAGIC * Access Control Level
# MAGIC * External Locations
# MAGIC * Automated Data Lineage
# MAGIC * Highly accessible Audit Logs

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Access Control Level
# MAGIC * Table ACL permissions, leveraging users, group and table across multiple workspaces.
# MAGIC * Advanced Permissions through Dynamic Views & Data Masking

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.1. Grant table ACL using standard SQL GRANT on all the objects (CATALOG, SCHEMA, TABLE)

# COMMAND ----------

# MAGIC %run ./_resources/00-init

# COMMAND ----------

# Create the CATALOG
print(f"The demo will create and use the catalog {catalog}:")
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- the catalog has been created for your user and is defined as default. All shares will be created inside.
# MAGIC -- make sure you run the 00-setup cell above to init the catalog to your user. 
# MAGIC SELECT CURRENT_CATALOG();

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the SCHEMA
# MAGIC CREATE SCHEMA IF NOT EXISTS uc_acl;
# MAGIC USE uc_acl;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the TABLE
# MAGIC CREATE TABLE IF NOT EXISTS uc_acl.customers (
# MAGIC   id BIGINT,
# MAGIC   creation_date STRING,
# MAGIC   customer_firstname STRING,
# MAGIC   customer_lastname STRING,
# MAGIC   country STRING,
# MAGIC   customer_email STRING,
# MAGIC   address STRING,
# MAGIC   gender DOUBLE,
# MAGIC   age_group DOUBLE); 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Load some data
# MAGIC COPY INTO uc_acl.customers  FROM '/demo/uc/users' FILEFORMAT=JSON 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Read data
# MAGIC SELECT * FROM  uc_acl.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant user access
# MAGIC -- Let's grant all users a SELECT
# MAGIC GRANT SELECT ON TABLE uc_acl.customers TO `account users`;
# MAGIC 
# MAGIC -- We'll grant an extra MODIFY to our Data Engineer
# MAGIC GRANT SELECT, MODIFY ON TABLE uc_acl.customers TO `dataengineers`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON TABLE uc_acl.customers

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.2. Advanced Permissions to mask sensitive PII information, or restrict access to a subset of data without having to create and maintain multiple tables (based on a specific field).

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC We'll be using the previous customers table.
# MAGIC 
# MAGIC As you can see, this table has a `country`field. We want to be able to restrict the table access based in this country.
# MAGIC 
# MAGIC Data Analyst and Data Scientists in USA can only access the local Dataset, same for the FR team.

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Option 1: Using Groups
# MAGIC One option to do that would be to create groups in the Unity Catalog. We can call the groups:
# MAGIC * `ANALYST_FR`
# MAGIC * `ANALYST_USA`. 
# MAGIC 
# MAGIC You can then add a view with `CASE ... WHEN` statement based on your groups to define when the data can be accessed.
# MAGIC 
# MAGIC The `is_member()` function combines with a column can dynamically check access based on the row.

# COMMAND ----------

# DBTITLE 1,Getting the Current User
# MAGIC %sql
# MAGIC SELECT current_user();

# COMMAND ----------

# DBTITLE 1,Am I member of the ADMIN group defined at the users level?
# MAGIC %sql
# MAGIC SELECT is_account_group_member('account users'), is_account_group_member('ADMIN');

# COMMAND ----------

# MAGIC %sql
# MAGIC select group_name, is_member(group_name) from (
# MAGIC   select CONCAT("ANALYST_", country) as group_name from uc_acl.customers)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Although we are not admin of 
# MAGIC CREATE VIEW IF NOT EXISTS uc_acl.customer_dynamic_view  AS (
# MAGIC   SELECT * FROM uc_acl.customers as customers WHERE is_account_group_member(CONCAT("ANALYST_", country))
# MAGIC );
# MAGIC -- Then grant select access on the view only
# MAGIC GRANT SELECT ON VIEW uc_acl.customer_dynamic_view TO `account users`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- As we are not part of any ANALYST_<country> group, the table will appear be empty. Adding ourself to the ANALYST_FR group will give us access to the FR entries only.
# MAGIC select * from uc_acl.customer_dynamic_view

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Option 2: Dynamic Views 
# MAGIC Dynamic views can also be used to add data masking. For this example we'll be using the `current_user()` functions.
# MAGIC 
# MAGIC Let's create a table with all our current analyst permission including a GDPR permission flag: `analyst_permissions`.
# MAGIC 
# MAGIC This table has 3 field:
# MAGIC 
# MAGIC * `analyst_email`: to identify the analyst (we could work with groups instead)
# MAGIC * `country_filter`: we'll filter the dataset based on this value
# MAGIC * `gdpr_filter`: if true, we'll filter the PII information from the table. If not set the user can see all the information
# MAGIC 
# MAGIC *Of course this could be implemented with the previous `is_account_group_member()` function instead of individual users information being saved in a permission tale.*
# MAGIC 
# MAGIC Let's query this table and check our current user permissions. As you can see I don't have GDPR filter enabled and a filter on FR is applied for me in the permission table we created.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uc_acl.analyst_permissions where analyst_email = current_user()

# COMMAND ----------

# DBTITLE 1,Let's create the secure view to filter PII information and country based on the analyst permission
# MAGIC %sql
# MAGIC CREATE VIEW IF NOT EXISTS uc_acl.customer_dynamic_view_gdpr AS (
# MAGIC   SELECT 
# MAGIC   id ,
# MAGIC   creation_date,
# MAGIC   country,
# MAGIC   gender,
# MAGIC   age_group,
# MAGIC   CASE WHEN country.gdpr_filter=1 THEN sha1(customer_firstname) ELSE customer_firstname END AS customer_firstname,
# MAGIC   CASE WHEN country.gdpr_filter=1 THEN sha1(customer_lastname)  ELSE customer_lastname  END AS customer_lastname,
# MAGIC   CASE WHEN country.gdpr_filter=1 THEN sha1(customer_email)     ELSE customer_email     END AS customer_email
# MAGIC   FROM 
# MAGIC     uc_acl.customers as customers INNER JOIN 
# MAGIC     uc_acl.analyst_permissions country  ON country_filter=country
# MAGIC   WHERE 
# MAGIC     country.analyst_email=current_user() 
# MAGIC );
# MAGIC -- Then grant select access on the view only
# MAGIC GRANT SELECT ON VIEW uc_acl.customer_dynamic_view_gdpr TO `account users`;

# COMMAND ----------

# DBTITLE 1,Because I've a filter on COUNTRY=FR and gdpr_filter=0, I'll see all the FR customers information.
# MAGIC %sql
# MAGIC select * from uc_acl.customer_dynamic_view_gdpr 

# COMMAND ----------

# DBTITLE 1,We'll enable the gdpr_filter flag and change our country_filter to USA.
# MAGIC %sql
# MAGIC UPDATE uc_acl.analyst_permissions SET country_filter='USA', gdpr_filter=1 where analyst_email=current_user();
# MAGIC 
# MAGIC select * from uc_acl.customer_dynamic_view_gdpr ;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Working with External Locations to operate security at scale, cross workspace, and be ready to build data mesh setups.
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/external/uc-external-location.png" style="float:right; margin-left:10px" width="800"/>
# MAGIC 
# MAGIC 
# MAGIC Accessing external cloud storage is easily done using `External locations`.
# MAGIC 
# MAGIC This can be done using 3 simple SQL command:
# MAGIC 
# MAGIC 
# MAGIC 1. First, create a Storage credential. I'll contain the IAM role/SP required to access your cloud storage
# MAGIC 1. Create an External location using your Storage credential. It can be any cloud location (a sub folder)
# MAGIC 1. Finally, Grant permissions to your users to access this Storage Credential

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.1. Create a storage credential
# MAGIC To do that, we'll use Databricks Unity Catalog UI:
# MAGIC 
# MAGIC 1. Open the Data Explorer in DBSQL
# MAGIC 1. Select the "Storage Credential" menu
# MAGIC 1. Click on "Create Credential"
# MAGIC 1. Fill your credential information: the name and IAM role you will be using
# MAGIC 
# MAGIC Because you need to be ADMIN, this step has been created for you.
# MAGIC 
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/external/uc-external-location-cred.png" width="400"/>

# COMMAND ----------

# DBTITLE 1,For our demo, let's make sure all users can alter this storage credential:
# MAGIC %sql
# MAGIC ALTER STORAGE CREDENTIAL `field_demos_credential`  OWNER TO `account users`;
# MAGIC DESCRIBE STORAGE CREDENTIAL `field_demos_credential`;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.2. Create an external location
# MAGIC We'll then create our `EXTERNAL LOCATION` using the following path:<br/>
# MAGIC `s3a://databricks-e2demofieldengwest/external_location/`
# MAGIC 
# MAGIC Note that you need to be Account Admin to do that, it'll fail with a permission error if you are not. But don't worry, the external location has been created for you.
# MAGIC 
# MAGIC You can also update your location using SQL operations:
# MAGIC <br/>
# MAGIC ```ALTER EXTERNAL LOCATION `xxxx`  RENAME TO `yyyy`; ```<br/>
# MAGIC ```DROP EXTERNAL LOCATION IF EXISTS `xxxx`; ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Note: you need to be account ADMIN to run this and create the external location.
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `field_demos_external_location`
# MAGIC   URL 's3a://databricks-e2demofieldengwest/external_location/' 
# MAGIC   WITH (CREDENTIAL `field_demos_credential`)
# MAGIC   COMMENT 'External Location for demos' ;
# MAGIC 
# MAGIC -- let's make everyone owner for the demo to be able to change the permissions easily. DO NOT do that for real usage.
# MAGIC ALTER EXTERNAL LOCATION `field_demos_external_location`  OWNER TO `account users`;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTERNAL LOCATION `field_demos_external_location`;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3. Grant permissions to users/ groups

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `field_demos_external_location` TO `account users`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Data Lineage
# MAGIC * Table Lineage
# MAGIC * Column Lineage

# COMMAND ----------

# DBTITLE 1,Generate some data to show automated lineage graphs
# MAGIC %sql
# MAGIC -- Create a Delta Table In Unity Catalog
# MAGIC CREATE DATABASE IF NOT EXISTS uc_lineage;
# MAGIC CREATE TABLE IF NOT EXISTS uc_lineage.menu (recipe_id INT, app string, main string, desert string);
# MAGIC DELETE from uc_lineage.menu ;
# MAGIC 
# MAGIC INSERT INTO uc_lineage.menu 
# MAGIC     (recipe_id, app, main, desert) 
# MAGIC VALUES 
# MAGIC     (1,"Ceviche", "Tacos", "Flan"),
# MAGIC     (2,"Tomato Soup", "Souffle", "Creme Brulee"),
# MAGIC     (3,"Chips","Grilled Cheese","Cheescake");
# MAGIC     
# MAGIC -- To show dependancies between tables, we create a new one `AS SELECT` from the previous one, concatenating three columns into a new one
# MAGIC CREATE TABLE IF NOT EXISTS uc_lineage.dinner 
# MAGIC   AS SELECT recipe_id, concat(app," + ", main," + ",desert) as full_menu FROM uc_lineage.menu;

# COMMAND ----------

# MAGIC %md
# MAGIC The last step is to create a third table as a join from the two previous ones. This time we will use Python instead of SQL.
# MAGIC 
# MAGIC - We create a Dataframe with some random data formatted according to two columns, `id` and `recipe_id`
# MAGIC - We save this Dataframe as a new table, `main.lineage.price`
# MAGIC - We read as two Dataframes the previous two tables, `main.lineage.dinner` and `main.lineage.price`
# MAGIC - We join them on `recipe_id` and save the result as a new Delta table `main.lineage.dinner_price`

# COMMAND ----------

df = spark.range(3).withColumn("price", F.round(10*F.rand(seed=42),2)).withColumnRenamed("id", "recipe_id")

df.write.mode("overwrite").saveAsTable("uc_lineage.price")

dinner = spark.read.table("uc_lineage.dinner")
price = spark.read.table("uc_lineage.price")

dinner_price = dinner.join(price, on="recipe_id")
dinner_price.write.mode("overwrite").saveAsTable("uc_lineage.dinner_price")

# COMMAND ----------

# MAGIC %md 
# MAGIC Let's head over to the UI to see lineage in action!

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Audit Logs

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4.1. Audit Log Ingestion

# COMMAND ----------

# DBTITLE 1,Accessing Raw Log Data
display(dbutils.fs.ls("s3a://databricks-field-eng-audit-logs/e2-demo-field-eng"))

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG field_demos;
# MAGIC CREATE DATABASE IF NOT EXISTS field_demos.audit_log;

# COMMAND ----------

# DBTITLE 1,Ingest the raw JSON from the audit log service as our bronze table
from pyspark.sql.functions import udf, col, from_unixtime, from_utc_timestamp, from_json, map_filter

def ingest_audit_log():
  print("starting audit log ingestion...")
  streamDF = (spark.readStream.format("cloudFiles")
                    .option("cloudFiles.format", "json")
                    .option("cloudFiles.inferColumnTypes", True)
                    .option("cloudFiles.useIncrementalListing", True)
                    .option("cloudFiles.schemaHints", "requestParams map<string,string>")
                    .option("cloudFiles.schemaEvolutionMode","addNewColumns")
                    .option("cloudFiles.schemaLocation", '/demo/uc/audit_log/bronze/audit_log_schema')
                    .load('s3a://databricks-field-eng-audit-logs/e2-demo-field-eng'))
  (streamDF.writeStream
            .outputMode("append")
            .option("checkpointLocation", '/demo/uc/audit_log/bronze/checkpoint') 
            .option("mergeSchema", True)
            .partitionBy("workspaceId")
            .trigger(availableNow=True)
            .toTable('field_demos.audit_log.bronze').awaitTermination())

def ingest_and_restart_on_new_col():
  try: 
    ingest_audit_log()
  except BaseException as e:
    #Adding a new column will trigger an UnknownFieldException. In this case we just restart the stream:
    stack = str(e.stackTrace)
    if 'UnknownFieldException' in stack:
      print(f"new colunm, restarting ingestion: {stack}")
      ingest_and_restart_on_new_col()
    else:
      raise e
ingest_and_restart_on_new_col()

# COMMAND ----------

# DBTITLE 1,Audit log is enabled on all the workspaces.
# MAGIC %sql select distinct(workspaceId) from field_demos.audit_log.bronze order by workspaceId

# COMMAND ----------

# DBTITLE 1,Audit log is enabled for all services, including "unityCatalog"
# MAGIC %sql select distinct(serviceName) from field_demos.audit_log.bronze order by serviceName desc

# COMMAND ----------

# MAGIC %python
# MAGIC #let's make sure everybody can access the tables created in the demo, and enable auto compaction for future run as this is a real pipeline
# MAGIC for service_name in service_name_list + ['bronze']:
# MAGIC   table_name = get_table_name(service_name)
# MAGIC   spark.sql(f"GRANT ALL PRIVILEGES ON TABLE field_demos.audit_log.{table_name} TO `account users`")
# MAGIC   spark.sql(f"ALTER TABLE field_demos.audit_log.{table_name} SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG field_demos;

# COMMAND ----------

# DBTITLE 1,What types of Actions are captured by the Audit Logs?
# MAGIC %sql
# MAGIC -- Now we are ready to do some data analysis on our audit log!
# MAGIC SELECT
# MAGIC   distinct actionName
# MAGIC FROM
# MAGIC   audit_log.unitycatalog
# MAGIC ORDER BY
# MAGIC   actionName

# COMMAND ----------

# DBTITLE 1,What are the most popular Actions?
# MAGIC %sql
# MAGIC SELECT
# MAGIC   actionName,
# MAGIC   count(actionName) as actionCount
# MAGIC FROM
# MAGIC   audit_log.unitycatalog
# MAGIC GROUP BY
# MAGIC   actionName
# MAGIC ORDER BY actionCount DESC

# COMMAND ----------

# DBTITLE 1,Tracking UC Table Query Requests
# MAGIC %sql
# MAGIC SELECT
# MAGIC   date_time,
# MAGIC   email,
# MAGIC   actionName,
# MAGIC   requestParams.operation as operation,
# MAGIC   requestParams.is_permissions_enforcing_client as pe_client,
# MAGIC   requestParams.table_full_name as table_name,
# MAGIC   response.errorMessage as error
# MAGIC FROM
# MAGIC   audit_log.unitycatalog
# MAGIC WHERE
# MAGIC   actionName in ("generateTemporaryTableCredential")
# MAGIC ORDER BY
# MAGIC   date_time desc

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   email,
# MAGIC   date,
# MAGIC   requestParams.operation as operation,  
# MAGIC   requestParams.table_full_name as table_name,
# MAGIC   count(actionName) as queries
# MAGIC FROM
# MAGIC   audit_log.unitycatalog
# MAGIC where
# MAGIC   actionName in ("generateTemporaryTableCredential")
# MAGIC group by
# MAGIC   1,
# MAGIC   2,
# MAGIC   3,
# MAGIC   4
# MAGIC order by
# MAGIC   2 desc

# COMMAND ----------

# DBTITLE 1,Which Users are Most Active Overall?
# MAGIC %sql
# MAGIC SELECT
# MAGIC   email,
# MAGIC   count(actionName) AS numActions
# MAGIC FROM
# MAGIC   audit_log.unitycatalog
# MAGIC group by
# MAGIC   email
# MAGIC order by
# MAGIC   numActions desc
