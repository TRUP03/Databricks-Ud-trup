# Databricks notebook source
# MAGIC %md
# MAGIC ####Accessing ADLS through Service Principal
# MAGIC - Register Service Principal(App) into ADD, The client id and tenant_id will be created
# MAGIC - Generate a secret for that service principal(APP) - only generated once for each app
# MAGIC - Set spark config with client_id, tenant_id, secret
# MAGIC - Assign the role(Storage Blob data contributor) to the service principal

# COMMAND ----------

display(dbutils.secrets.listScopes())
display(dbutils.secrets.list('SS-databricks-trup'))

# COMMAND ----------

application_client_id = dbutils.secrets.get(scope="SS-databricks-trup", key="application-client-id")
dir_tenant_id = dbutils.secrets.get(scope="SS-databricks-trup", key="dir-tenant-id")
secret = dbutils.secrets.get(scope="SS-databricks-trup", key="secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_client_id,
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{dir_tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
    source="abfss://demo@dlstorageaccounttrup.dfs.core.windows.net",
    mount_point="/mnt/dlstorageaccounttrup/demo",
    extra_configs=configs,
)

# COMMAND ----------

display(dbutils.fs.ls("/mnt"))

# COMMAND ----------

display(spark.read.csv("/mnt/dlstorageaccounttrup/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/dlstorageaccounttrup/demo")

# COMMAND ----------

