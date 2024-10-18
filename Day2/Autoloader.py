# Databricks notebook source
(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format","csv")
 .option("cloudFiles.schemaLocation","/FileStore/tables/schemaloaction")
 .load("/Volumes/sony_dev/bronze/stream_in")
    .writeStream
    .option("checkpointLocation","/FileStore/tables/checkpoint_autoloader")
    .table("sony_dev.bronze.autoloader"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sony_dev.bronze.autoloader

# COMMAND ----------


