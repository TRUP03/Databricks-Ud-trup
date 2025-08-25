# Databricks notebook source
# MAGIC %run "../includes/1.folder-paths"

# COMMAND ----------

race_df = spark.read.parquet(f"{processed_path}/race")

# COMMAND ----------

# filtered_df = race_df.filter("race_year = 2019 and round= 3")
filtered_df = race_df.where("race_year = 2019 and round= 3")
display(filtered_df)


# COMMAND ----------

