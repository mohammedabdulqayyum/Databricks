# Databricks notebook source
adobe_json_data = {
	"id": "0001",
	"type": "donut",
	"name": "Cake",
	"ppu": 0.55,
	"batters":
		{
			"batter":
				[
					{ "id": "1001", "type": "Regular" },
					{ "id": "1002", "type": "Chocolate" },
					{ "id": "1003", "type": "Blueberry" },
					{ "id": "1004", "type": "Devil's Food" }
				]
		},
	"topping":
		[
			{ "id": "5001", "type": "None" },
			{ "id": "5002", "type": "Glazed" },
			{ "id": "5005", "type": "Sugar" },
			{ "id": "5007", "type": "Powdered Sugar" },
			{ "id": "5006", "type": "Chocolate with Sprinkles" },
			{ "id": "5003", "type": "Chocolate" },
			{ "id": "5004", "type": "Maple" }
		]
}

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
 
# Assuming `spark` is your SparkSession
# Convert the JSON data to a DataFrame
df = spark.createDataFrame([adobe_json_data])
 
# Explode the 'batter' and 'topping' arrays to create a flat structure
df_batters = df.selectExpr("id as donut_id", "type as donut_type", "name", "ppu", "explode(batters.batter) as batter")
df_toppings = df.selectExpr("id as donut_id", "explode(topping) as topping")
combined_df = df_batters.join(df_toppings, "donut_id", "outer")
combined_df.display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
 
# Assuming `spark` is your SparkSession
# Convert the JSON data to a DataFrame
df = spark.createDataFrame([adobe_json_data])
 
# Explode the 'batter' and 'topping' arrays to create a flat structure
df_batters = df.selectExpr("id as donut_id", "type as donut_type", "name", "ppu", "explode(batters.batter) as batter")
df_toppings = df.selectExpr("id as donut_id", "explode(topping) as topping")
combined_df = df_batters.join(df_toppings, "donut_id", "outer")
 
(combined_df.withColumn("batter_id",col("batter.id"))
 .withColumn("batter_type",col("batter.type"))
 .withColumn("topping_id",col("topping.id"))
 .withColumn("topping_type",col("topping.type"))
 .drop("batter","topping")
 .display())
