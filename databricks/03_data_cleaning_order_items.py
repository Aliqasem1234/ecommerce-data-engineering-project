# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

order_items_df = spark.read.option("header", True)\
    .option("inferSchema", True)\
    .csv("/Volumes/workspace/default/raw_data/order_items.csv")

display(order_items_df)

# COMMAND ----------

order_items_df = order_items_df.toDF(
    *[c.lower().strip() for c in order_items_df.columns]
)

display(order_items_df)

# COMMAND ----------

order_items_df.printSchema()

# COMMAND ----------

order_items_df = order_items_df.filter(
    (col("quantity") > 0) & (col("unit_price") > 0)
)

display(order_items_df)

# COMMAND ----------

order_items_df = order_items_df.withColumn(
    "subtotal",
    col("quantity") * col("unit_price")
)

display(order_items_df)

# COMMAND ----------

order_items_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in order_items_df.columns
]).show()

# COMMAND ----------

order_items_df.write.mode("overwrite").saveAsTable("clean_order_items")