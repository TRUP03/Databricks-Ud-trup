# Databricks notebook source
result = dbutils.notebook.run("1.Ingestion-into-processed",0,{"parameter_data_source":"Ergast API"})
print(result)

# COMMAND ----------

result = dbutils.notebook.run("2.Ingestion-race-file-to-pricessed-folder",0,{"parameter_data_source":"Ergast API"})
print(result)

# COMMAND ----------

result = dbutils.notebook.run("3.constructor-JSON-processed",0,{"parameter_data_source":"Ergast API"})
print(result)

# COMMAND ----------

result = dbutils.notebook.run("4.driver-nested-json",0,{"parameter_data_source":"Ergast API"})
print(result)

# COMMAND ----------

result = dbutils.notebook.run("5.Results-json-processed",0,{"parameter_data_source":"Ergast API"})
print(result)

# COMMAND ----------

result = dbutils.notebook.run("6.multiline-json-ingestion",0,{"parameter_data_source":"Ergast API"})
print(result)

# COMMAND ----------

result = dbutils.notebook.run("7.Ingestion-lap_times_folder-processed",0,{"parameter_data_source":"Ergast API"})
print(result)

# COMMAND ----------

result = dbutils.notebook.run("8.Ingestion_qualifying_folder-processed",0,{"parameter_data_source":"Ergast API"})
print(result)