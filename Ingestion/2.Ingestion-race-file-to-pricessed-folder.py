# Databricks notebook source
# MAGIC %md
# MAGIC ####Steps for data ingestion into processed folder the race file
# MAGIC - Make a schema according to need and then read it.
# MAGIC - transformation - drop colunms, add columns, concat columns.
# MAGIC - write into sink

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
race_schema = StructType([
    StructField("raceId",IntegerType(),False),
    StructField("year",IntegerType(),True),
    StructField("round",IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name",StringType(),True),
    StructField("date",DateType(), True),
    StructField("time",StringType(),True),
    StructField("url",StringType(),True)
])

# COMMAND ----------

# display(dbutils.fs.ls('/mnt/dlstorageaccounttrup/raw'))
race_df = spark.read\
    .option('header', True)\
    .schema(race_schema)\
    .csv(f'{raw_path}/races.csv')
# display(race_df)

# COMMAND ----------

from pyspark.sql.functions import col
selected_df = race_df.select(col("raceId"), col("year"), col("round"), col("circuitId"), col("name"), col("date"),col('time'))
#either of these methods can be used
# selected_df = race_df.drop(col('url'))

# display(selected_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat
renamed_df = selected_df.withColumnRenamed("raceId","race_id")\
                        .withColumnRenamed("year","race_year")\
                        .withColumnRenamed("circuitId","circuit_id")\
                        .withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(" "),col("time")),"yyyy-MM-dd HH:mm:ss"))

# display(renamed_df)

renamed_df = ingestion_date(renamed_df)
renamed_df = renamed_df.withColumn("data_source",lit(var_data_source))

# COMMAND ----------

renamed_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/dlstorageaccounttrup/processed/race')

# COMMAND ----------

# display(spark.read.parquet('/mnt/dlstorageaccounttrup/processed/race'))

# COMMAND ----------

dbutils.notebook.exit('Success')