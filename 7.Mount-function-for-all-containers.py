# Databricks notebook source
def mountContainer(storageAccount, container):
    #these are for the service principal created
    application_client_id = dbutils.secrets.get(scope="SS-databricks-trup", key="application-client-id")
    dir_tenant_id = dbutils.secrets.get(scope="SS-databricks-trup", key="dir-tenant-id")
    secret = dbutils.secrets.get(scope="SS-databricks-trup", key="secret")

    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_client_id,
          "fs.azure.account.oauth2.client.secret": secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{dir_tenant_id}/oauth2/token"}

    if any(mount.mountPoint == f"/mnt/{storageAccount}/{container}" for mount in dbutils.fs.mounts()): 
        dbutils.fs.unmount(f"/mnt/{storageAccount}/{container}")

    dbutils.fs.mount(
    source=f"abfss://{container}@{storageAccount}.dfs.core.windows.net",
    mount_point=f"/mnt/{storageAccount}/{container}",
    extra_configs=configs
    )

    display(dbutils.fs.mounts())


# COMMAND ----------

mountContainer('dlstorageaccounttrup','raw')
mountContainer('dlstorageaccounttrup','presentation')
mountContainer('dlstorageaccounttrup','processed')

# COMMAND ----------

