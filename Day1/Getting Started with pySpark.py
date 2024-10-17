# Databricks notebook source
# DBTITLE 1,sql
print("hello")

# COMMAND ----------

# MAGIC %md
# MAGIC Spark Core.
# MAGIC RDD DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC DataFrame: Structured API

# COMMAND ----------

data = [(1,'a',20),(2,'b',30)]
schema = ["id", "name", "age"]
df = spark.createDataFrame(data, schema)
df.display()

# COMMAND ----------

data = [(1,'a',20),(2,'b',30)]
schema = "id int, name string, age int"
df = spark.createDataFrame(data, schema)
df.display()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC DataFrame Function
# MAGIC [.select,.alias,.withColumnRenamed,.withColumnsRenamed]
# MAGIC
# MAGIC Functions
# MAGIC [col]

# COMMAND ----------

df.select("*")

# COMMAND ----------

df.select("*").display()

# COMMAND ----------

df.select("id","age").display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(col("id").alias("emp_id")).display()

# COMMAND ----------

help(df.withColumnRenamed)

# COMMAND ----------

df.withColumnRenamed("id","empid").display()

# COMMAND ----------

df.withColumnsRenamed({"id":"emp_id","name":"emp_name","age":"emp_age"}).display()

# COMMAND ----------

df.withColumn("current_date", current_date()).display()

# COMMAND ----------

df.withColumn("age",current_date()).display()

# COMMAND ----------


