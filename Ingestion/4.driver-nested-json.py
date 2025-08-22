# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingestion of driver file into processed folder
# MAGIC - Read it, use schema created by you
# MAGIC - renamed, add and concat columns
# MAGIC - Write into folder

# COMMAND ----------

# MAGIC %run "../includes/1.folder-paths"

# COMMAND ----------

# MAGIC %run "../includes/2.common-functions"

# COMMAND ----------

# dbutils.widgets.help()
dbutils.widgets.text("parameter_data_source", "")
var_data_source = dbutils.widgets.get("parameter_data_source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
name_schema = StructType([
  StructField('forename',StringType(),True),
  StructField('surname',StringType(),True)
])

driver_schema = StructType([
  StructField('driverId',IntegerType(),False),
  StructField('driverRef',StringType(),True),
  StructField('number',StringType(),True),
  StructField('code',StringType(),True),
  StructField('name',name_schema),
  StructField('dob',DateType(),True),
  StructField('nationality',StringType(),True),
  StructField('url',StringType(),True),
])

# COMMAND ----------

driver_df = spark.read\
    .schema(driver_schema)\
    .json(f'{raw_path}/drivers.json')

# display(driver_df)

# COMMAND ----------

driver_dropped_df = driver_df.drop('url')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit,concat,col
driver_transformed_df = driver_dropped_df.withColumnRenamed('driverId','driver_id')\
                                 .withColumnRenamed('driverRef','driver-ref')\
                                 .withColumn('name',concat(col("name.forename"), lit(" "), col("name.surname")))
driver_transformed_df = ingestion_date(driver_transformed_df)
driver_transformed_df = driver_transformed_df.withColumn('data_source',lit(var_data_source))
# display(driver_transformed_df)

# COMMAND ----------

driver_transformed_df.write.mode('overwrite').parquet('/mnt/dlstorageaccounttrup/processed/driver')

# COMMAND ----------

# display(spark.read.parquet('/mnt/dlstorageaccounttrup/processed/driver'))

# COMMAND ----------

dbutils.notebook.exit('Success')