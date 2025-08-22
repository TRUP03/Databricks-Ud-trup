# Databricks notebook source
# MAGIC %run "../includes/1.folder-paths"

# COMMAND ----------

# MAGIC %run "../includes/2.common-functions"

# COMMAND ----------

# dbutils.widgets.help()
dbutils.widgets.text("parameter_data_source", "")
var_data_source = dbutils.widgets.get("parameter_data_source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
qualify_schema = StructType([
    StructField("qualifyId",IntegerType(),False),
    StructField("raceId",IntegerType(),True),
    StructField("driverId",IntegerType(),True),
    StructField("constructorId",IntegerType(),True),
    StructField("number",IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1",StringType(),True),
    StructField("q2",StringType(), True),
    StructField("q3",StringType(),True)
])

# COMMAND ----------

# display(dbutils.fs.ls('/mnt/dlstorageaccounttrup/raw'))
qualify_df = spark.read\
    .schema(qualify_schema)\
    .option('multiline',True)\
    .json(f'{raw_path}/qualifying')
# display(qualify_df)

# COMMAND ----------


from pyspark.sql.functions import current_timestamp,lit
renamed_df = qualify_df.withColumnRenamed("qualifyId","qualify_id")\
                        .withColumnRenamed("raceId","race_id")\
                        .withColumnRenamed("driverId","driver_id")\
                        .withColumnRenamed("constructorId","construnctor_id")

renamed_df =ingestion_date(renamed_df)
renamed_df = renamed_df.withColumn('date_source',lit(var_data_source))

# display(renamed_df)

# COMMAND ----------

renamed_df.write.mode('overwrite').parquet('/mnt/dlstorageaccounttrup/processed/qualifying')

# COMMAND ----------

# display(spark.read.parquet('/mnt/dlstorageaccounttrup/processed/qualifying'))

# COMMAND ----------

dbutils.notebook.exit('Success')