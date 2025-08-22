# Databricks notebook source
# MAGIC %run "../includes/1.folder-paths"

# COMMAND ----------

# MAGIC %run "../includes/2.common-functions"

# COMMAND ----------

# dbutils.widgets.help()
dbutils.widgets.text("parameter_data_source", "")
var_data_source = dbutils.widgets.get("parameter_data_source")

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
pit_stops_schema = StructType([
        StructField('raceId',IntegerType(),False),
        StructField('driverId',IntegerType(),True),
        StructField('stop',StringType(),True),
        StructField('lap',IntegerType(),True),
        StructField('time',StringType(),True),
        StructField('duration',StringType(),True),
        StructField('milliseconds',IntegerType(),True)
    ])

# COMMAND ----------

pit_stops_df = spark.read\
    .schema(pit_stops_schema)\
    .option('multiline',True)\
    .json(f'{raw_path}/pit_stops.json')

# display(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
pit_stops_final_df = pit_stops_df.withColumnRenamed('raceId','race_id')\
                                 .withColumnRenamed('driverId','driver_id')

pit_stops_final_df = ingestion_date(pit_stops_final_df)
pit_stops_final_df = pit_stops_final_df.withColumn('data_source',lit(var_data_source))
                    
# display(pit_stops_final_df)

# COMMAND ----------

pit_stops_final_df.write.mode('overwrite').parquet('/mnt/dlstorageaccounttrup/processed/pit_stops')

# COMMAND ----------

# display(spark.read.parquet('/mnt/dlstorageaccounttrup/processed/pit_stops'))

# COMMAND ----------

dbutils.notebook.exit('Success')