# Databricks notebook source
schema = "Id int, Name string, Gender string, Country string, date string"

# COMMAND ----------

(spark
 .readStream
 .schema(schema)
 .csv("/Volumes/sony_dev/bronze/stream_in",header=True)
    .writeStream
    .option("checkpointLocation","/FileStore/tables/checkpoint")
    .trigger(once=True)
    .table("sony_dev.bronze.stream")
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sony_dev.bronze.stream

# COMMAND ----------


