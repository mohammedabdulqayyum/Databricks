# Databricks notebook source
# MAGIC %run /Workspace/Users/mabdulqayyum2002@gmail.com/Day1/includes

# COMMAND ----------

# DBTITLE 1,Read
df_sales = spark.read.csv(f"{input_path}order_dates.csv",header =True, inferSchema=True)

# COMMAND ----------

df2 = add_ingestion(df_sales)

# COMMAND ----------

# DBTITLE 1,Write
df2.write.mode("overwrite").saveAsTable("order_dates")
