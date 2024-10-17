# Databricks notebook source
dbutils.widgets.text("environment","dev")
v=dbutils.widgets.get("environment")

# COMMAND ----------

# MAGIC %run /Workspace/Users/mabdulqayyum2002@gmail.com/Databricks/Day1/includes

# COMMAND ----------

# DBTITLE 1,Read
df_sales = spark.read.csv(f"{input_path}order_dates.csv",header =True, inferSchema=True)

# COMMAND ----------

df2 = add_ingestion(df_sales)

# COMMAND ----------

df3 = df2.withColumn("environment",lit(v))

# COMMAND ----------

# DBTITLE 1,Write
df3.write.mode("overwrite").saveAsTable("order_dates")

# COMMAND ----------

df3.display()

# COMMAND ----------


