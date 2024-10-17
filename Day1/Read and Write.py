# Databricks notebook source
df = spark.read.csv("/Volumes/maq_databricks/default/raw/sales.csv",header =True, inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df_customers = spark.read.json("/Volumes/maq_databricks/default/raw/customers.json")

# COMMAND ----------

df_customers.display()

# COMMAND ----------

df_customers.write.saveAsTable("customer")

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("sales")

# COMMAND ----------


