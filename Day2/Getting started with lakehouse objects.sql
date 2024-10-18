-- Databricks notebook source
use catalog sony_dev;

-- COMMAND ----------

create schema bronze;

-- COMMAND ----------

use schema bronze

-- COMMAND ----------

create table emp(id int,name string,age int)

-- COMMAND ----------

describe detail sony_dev.bronze.emp

-- COMMAND ----------

describe extended sony_dev.bronze.emp

-- COMMAND ----------

insert into emp values(1,'a',30);
insert into emp values(2,'b',31);
insert into emp values(3,'c',35);
insert into emp values(4,'d',38),(5,'e',40),(6,'f',42);

-- COMMAND ----------

select * from emp

-- COMMAND ----------

create view emp3 as select * from emp where id>3

-- COMMAND ----------

create or replace temporary view emp_temp as select * from emp where id=2;

-- COMMAND ----------

create or replace global temporary view emp_temp as select * from emp where id=2;

-- COMMAND ----------

select * from emp_temp;

-- COMMAND ----------

show views

-- COMMAND ----------

show views in global_temp

-- COMMAND ----------

select * from emp

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.csv("/Volumes/maq_databricks/default/raw/sales.csv",header=True,inferSchema=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.createOrReplaceTempView("sales_temp")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.createOrReplaceGlobalTempView("sales_global_temp")

-- COMMAND ----------

select * from sales_temp

-- COMMAND ----------

select * from global_temp.sales_global_temp
