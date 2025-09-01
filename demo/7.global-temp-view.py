# Databricks notebook source
# MAGIC %run "../includes/1.folder-paths"

# COMMAND ----------

final_df = spark.read.parquet(f'{presentation_path}/race_results')\
                     .filter('race_year == 2020')

# COMMAND ----------

final_df.createOrReplaceGlobalTempView('global_temp_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.global_temp_view

# COMMAND ----------

