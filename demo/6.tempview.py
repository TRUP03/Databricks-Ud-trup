# Databricks notebook source
# MAGIC %run "../includes/1.folder-paths"

# COMMAND ----------

final_df = spark.read.parquet(f'{presentation_path}/race_results')

# COMMAND ----------

final_df.createTempView('temp_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from temp_view
# MAGIC where race_year = 2020
# MAGIC

# COMMAND ----------

race_year_var = 2020
result_2020 = spark.sql(f'select * from temp_view where race_year = {race_year_var}')

display(result_2020)

# COMMAND ----------

