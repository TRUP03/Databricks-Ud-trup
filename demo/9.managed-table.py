# Databricks notebook source
# MAGIC %run "../includes/1.folder-paths"

# COMMAND ----------

final_df = spark.read.parquet(f'{presentation_path}/race_results')

# COMMAND ----------

final_df.write.format('parquet').saveAsTable("demo.race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC use demo

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.race_results where race_year = 2020

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE RACE_YEAR_2020_SQL
# MAGIC AS
# MAGIC select * from demo.race_results where race_year = 2020

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP table demo.race_year_2020_SQL

# COMMAND ----------

