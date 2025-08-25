# Databricks notebook source
# MAGIC %run "../includes/1.folder-paths"

# COMMAND ----------

from pyspark.sql.functions import col,count
race_df = spark.read.parquet(f'{processed_path}/race')\
    .filter(col('race_year') == 2020)\
    .withColumnRenamed('name','race_name')
race_df.count()
display(race_df)

# COMMAND ----------

circuit_df = spark.read.parquet(f'{processed_path}/circuit')\
    .withColumnRenamed('name','circuit_name')\
    .filter("circuit_id<=65")
display(circuit_df)
circuit_df.count()

# COMMAND ----------

# race_circuit_left_df = race_df.join(circuit_df, race_df.circuit_id == circuit_df.circuit_id,"right")\
#     .select(race_df.race_id,race_df.race_year,race_df.round,race_df.circuit_id,race_df.race_name, circuit_df.circuit_name)
# display(race_circuit_left_df)

# COMMAND ----------

# race_circuit_df = race_df.join(circuit_df,race_df.circuit_id == circuit_df.circuit_id,'inner')\
#     .select(race_df.race_id,race_df.race_year,race_df.round,race_df.circuit_id,race_df.race_name, circuit_df.circuit_name)
# display(race_circuit_df)

# COMMAND ----------

race_circuit_semi_df = race_df.join(circuit_df, race_df.circuit_id == circuit_df.circuit_id, 'semi')
display(race_circuit_semi_df)

# COMMAND ----------

race_circuit_semi_df = race_df.join(circuit_df, race_df.circuit_id== circuit_df.circuit_id, 'anti')
display(race_circuit_semi_df)

# COMMAND ----------

cross_df = race_df.crossJoin(circuit_df)
display(cross_df)

# COMMAND ----------

