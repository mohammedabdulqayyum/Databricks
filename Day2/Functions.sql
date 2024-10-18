-- Databricks notebook source
CREATE OR REPLACE FUNCTION sale_announcement(item_name STRING, item_price INT)
RETURNS STRING
RETURN concat("The ", item_name," is on sale for $",round(item_price*0.2,0))

-- COMMAND ----------

select * from maq_databricks.default.sales

-- COMMAND ----------

select *, sale_announcement(product_id, total_amount) as discountat20 from maq_databricks.default.sales
