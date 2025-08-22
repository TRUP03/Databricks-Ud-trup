# Databricks notebook source
# MAGIC %run "../includes/1.folder-paths"

# COMMAND ----------

# MAGIC %run "../includes/2.common-functions"

# COMMAND ----------

dbutils.widgets.help()
dbutils.widgets.text("parameter_data_source", "")
var_data_source = dbutils.widgets.get("parameter_data_source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuit_schema = StructType(
    [
        StructField('circuitId',IntegerType(), False),
        StructField('circuitRef',StringType(),True),
        StructField('name',StringType(),True),
        StructField('location',StringType(), True),
        StructField('country',StringType(),True),
        StructField('lat',DoubleType(),True),
        StructField('lng',DoubleType(),True),
        StructField('alt',IntegerType(),True),
        StructField('url',StringType(),True)
    ]
)

# COMMAND ----------

circuit_df_schema = spark.read\
    .option('header',True)\
    .schema(circuit_schema)\
    .csv(f'{raw_path}/circuits.csv')
        #   dbfs:/mnt/dlstorageaccounttrup/raw/circuits.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ####To select First is used when we just need to select the columns without doing any operations on it.
# MAGIC operations like alias("") 

# COMMAND ----------

selected_df = circuit_df_schema.select("circuitId","circuitRef","name","location","country","lat","lng","alt")
# display(selected_df)

# COMMAND ----------

# selected_df = circuit_df_schema.select(circuit_df_schema.circuitId.alias("Id"),circuit_df_schema.circuitRef,circuit_df_schema.name, circuit_df_schema.location, circuit_df_schema.country)
# display(selected_df)

# COMMAND ----------

# selected_df = circuit_df_schema.select(circuit_df_schema["circuitId"].alias("Id"),circuit_df_schema["circuitRef"],circuit_df_schema["name"])
# display(selected_df)

# COMMAND ----------

# from pyspark.sql.functions import col
# selected_circuit_df = circuit_df_schema.select(col("circuitId"), col("circuitRef"),col("name"), col("location"), col("lat"), col("lng"), col("alt"))
# display(selected_circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####2)Renaming columns
# MAGIC

# COMMAND ----------

circuit_renamed_columns = selected_df\
  .withColumnRenamed("circuitId","circuit_id")\
  .withColumnRenamed("circuitRef","circuit_ref")\
  .withColumnRenamed("lat","lattitude")\
  .withColumnRenamed("lng","longitude")\
  .withColumnRenamed("alt","altitude")

# display(circuit_renamed_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC #####3)Adding Ingestion date column

# COMMAND ----------


from pyspark.sql.functions import lit
circuit_final_df = ingestion_date(circuit_renamed_columns)
circuit_final_df = circuit_final_df.withColumn("data_source",lit(var_data_source))

# circuit_final_df = circuit_final_df.select(col("circuit_id"),col("circuit_ref"), col("name"), col("location"),
#                                            col("lattitude"), col("longitude"),col("altitude"), col("ingestion_date"))
# display(circuit_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####4) Writing data to processed folder in parquet format.

# COMMAND ----------

circuit_final_df.write.mode('overwrite').parquet('/mnt/dlstorageaccounttrup/processed/circuit')

# COMMAND ----------

display(spark.read.parquet('/mnt/dlstorageaccounttrup/processed/circuit'))

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

