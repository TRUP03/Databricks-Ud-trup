# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingestion of results
# MAGIC - Creation of schema
# MAGIC - read it
# MAGIC - transform and rename, add, drop
# MAGIC - Write

# COMMAND ----------

# MAGIC %run "../includes/2.common-functions"

# COMMAND ----------

# MAGIC %run "../includes/1.folder-paths"

# COMMAND ----------

# dbutils.widgets.help()
dbutils.widgets.text("parameter_data_source", "")
var_data_source = dbutils.widgets.get("parameter_data_source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType
result_schema = StructType([
    StructField('resultId',IntegerType(),False),
    StructField('raceId',IntegerType(),True),
    StructField('driverId',IntegerType(),True),
    StructField('constructorId',IntegerType(),True),
    StructField('number',IntegerType(),True),
    StructField('grid',IntegerType(),True),
    StructField('position',IntegerType(),True),
    StructField('positionText',StringType(),True),
    StructField('positionOrder',IntegerType(),True),
    StructField('points',IntegerType(),True),
    StructField('laps',IntegerType(),True),
    StructField('time',StringType(),True),
    StructField('milliseconds',IntegerType(),True),
    StructField('fastestLap',IntegerType(),True),
    StructField('rank',IntegerType(),True),
    StructField('fastestLapTime',StringType(),True),
    StructField('fastestLapSpeed',StringType(),True),
    StructField('statusId',IntegerType(),True),
])

# COMMAND ----------

result_df = spark.read\
            .schema(result_schema)\
            .json(f"{raw_path}/results.json")

# display(result_df)

# COMMAND ----------

result_transformed_df = result_df.drop('statusId')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
result_transformed_df = result_transformed_df.withColumnRenamed('resultId','result_id')\
                                 .withColumnRenamed('raceId','race_id')\
                                 .withColumnRenamed('driverId','driver_id')\
                                 .withColumnRenamed('constructorId','constructor_id')\
                                 .withColumnRenamed('fastestLap','fastest_lap')\
                                 .withColumnRenamed('fastestLapTime','fastest_lap_time')\
                                 .withColumnRenamed('fastestLapSpeed','fastest_lap_speed')\
                                 .withColumnRenamed('positionText','position_text')\
                                 .withColumnRenamed('positionOrder','position_order')

result_transformed_df = ingestion_date(result_transformed_df)
result_transformed_df = result_transformed_df.withColumn("data_source",lit(var_data_source))                                 
# display(result_transformed_df)

# COMMAND ----------

result_transformed_df.write.mode('overwrite').partitionBy('race_id').parquet('/mnt/dlstorageaccounttrup/results')

# COMMAND ----------

# display(spark.read.parquet('/mnt/dlstorageaccounttrup/results'))

# COMMAND ----------

dbutils.notebook.exit('Success')