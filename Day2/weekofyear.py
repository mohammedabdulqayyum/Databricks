# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists maq_databricks.gold

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table maq_databricks.gold.orders_weekofyear as
# MAGIC select week_of_year, count(*) as count from maq_databricks.default.order_dates group by week_of_year

# COMMAND ----------


