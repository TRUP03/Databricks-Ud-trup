# Databricks notebook source
# MAGIC %md
# MAGIC ##Access ADLS through access key
# MAGIC - --spark config set up
# MAGIC - --list files from conatiner demo
# MAGIC - --read data from circuit file present in demo folder.

# COMMAND ----------

access_key = dbutils.secrets.get("SS-databricks-trup", "accesskey")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.dlstorageaccounttrup.dfs.core.windows.net",access_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dlstorageaccounttrup.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@dlstorageaccounttrup.dfs.core.windows.net/circuits.csv"))


# COMMAND ----------

