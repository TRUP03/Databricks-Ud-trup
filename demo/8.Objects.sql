-- Databricks notebook source
select CURRENT_DATABASE()

-- COMMAND ----------

CREATE Database demo;

-- COMMAND ----------

show databases;

-- COMMAND ----------

describe database extended demo

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

use demo

-- COMMAND ----------

show tables