# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE customers as
# MAGIC SELECT *,current_timestamp() as ingestion_date FROM JSON.`/Volumes/maq_databricks/default/raw/customers.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table customer

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE product as
# MAGIC SELECT *,current_timestamp() as ingestion_date FROM JSON.`/Volumes/maq_databricks/default/raw/products.json`

# COMMAND ----------


