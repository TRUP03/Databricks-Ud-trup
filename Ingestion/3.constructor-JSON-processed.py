# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingestion of JSON file into Processed folder
# MAGIC - Build a schema using diff method, but using SturctType can also be done.
# MAGIC - Read json file
# MAGIC - Drop unwanted columns - rename those - transformation
# MAGIC - Write to destination folder

# COMMAND ----------

# MAGIC %run "../includes/1.folder-paths"

# COMMAND ----------

# MAGIC %run "../includes/2.common-functions"

# COMMAND ----------

# dbutils.widgets.help()
dbutils.widgets.text("parameter_data_source", "")
var_data_source = dbutils.widgets.get("parameter_data_source")

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING,nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read\
    .schema(constructors_schema)\
    .json(f'{raw_path}/constructors.json')

# display(constructor_df)

# COMMAND ----------

constructor_df = constructor_df.drop('url')
# display(constructor_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
constructor_final_df = constructor_df.withColumnRenamed('constructorId','constructor_id')\
                                     .withColumnRenamed('constructorRef','constructor_ref')


constructor_final_df = ingestion_date(constructor_final_df)
constructor_final_df = constructor_final_df.withColumn("data_source",lit(var_data_source))
# display(constructor_final_df)

# COMMAND ----------

constructor_final_df.write.mode('overwrite').parquet('/mnt/dlstorageaccounttrup/processed/constructor')

# COMMAND ----------

# display(spark.read.parquet('/mnt/dlstorageaccounttrup/processed/constructor'))

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

