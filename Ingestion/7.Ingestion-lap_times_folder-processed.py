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
lap_times_schema = StructType([
    StructField("raceId",IntegerType(),False),
    StructField("driverId",IntegerType(),True),
    StructField("lap",IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time",StringType(),True),
    StructField("milliseconds",IntegerType(), True)
])

# COMMAND ----------

# display(dbutils.fs.ls('/mnt/dlstorageaccounttrup/raw'))
lap_times_df = spark.read\
    .schema(lap_times_schema)\
    .csv(f'{raw_path}/lap_times')
# display(lap_times_df)

# COMMAND ----------


from pyspark.sql.functions import lit
renamed_df = lap_times_df.withColumnRenamed("raceId","race_id")\
                        .withColumnRenamed("driverId","driver_id")
renamed_df = ingestion_date(renamed_df)
renamed_df = renamed_df.withColumn('data_source',lit(var_data_source))
# display(renamed_df)

# COMMAND ----------

renamed_df.write.mode('overwrite').parquet('/mnt/dlstorageaccounttrup/processed/lap_times')

# COMMAND ----------

# display(spark.read.parquet('/mnt/dlstorageaccounttrup/processed/lap_times'))

# COMMAND ----------

dbutils.notebook.exit('Success')