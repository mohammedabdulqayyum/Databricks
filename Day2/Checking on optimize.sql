-- Databricks notebook source
select * from maq_databricks.default.customers

-- COMMAND ----------

describe history maq_databricks.default.customers

-- COMMAND ----------

delete from maq_databricks.default.customers where customer_id=5

-- COMMAND ----------

describe history maq_databricks.default.customers

-- COMMAND ----------

update maq_databricks.default.customers set customer_id=5 where customer_id=1

-- COMMAND ----------


