# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

customers_df = spark.table("clean_customers")
orders_df = spark.table("clean_orders")
order_items_df = spark.table("clean_order_items")
products_df = spark.table("clean_products")

final_df = orders_df \
    .join(customers_df, "customer_id") \
    .join(order_items_df, "order_id") \
    .join(products_df, "product_id") \
    .withColumn("revenue", col("quantity") * col("unit_price"))

# COMMAND ----------

items_df = final_df.withColumn(
    "item_struct",
    struct(
        "product_id",
        "product_name",
        "quantity",
        "unit_price",
        "revenue"
    )
)

# COMMAND ----------

orders_mongo_df = items_df.groupBy(
    "order_id",
    "order_date",
    "status",
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "country"
).agg(
    collect_list("item_struct").alias("items"),
    sum("revenue").alias("total_revenue")
)

# COMMAND ----------

orders_mongo_df = orders_mongo_df.withColumn(
    "customer",
    struct(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "country"
    )
).drop(
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "country"
)

# COMMAND ----------

display(orders_mongo_df)

# COMMAND ----------

# MAGIC %pip install pymongo

# COMMAND ----------

from pymongo import MongoClient

connection_string = "mongodb+srv://aliqasem606060_db_user:rwhi3VcLug2w6DJA@cluster0.duaelje.mongodb.net/"

client = MongoClient(connection_string)

db = client["ecommerce_analytics_db"]
collection = db["orders_embedded"]

# COMMAND ----------

orders_list = [row.asDict(recursive=True) for row in orders_mongo_df.collect()]

# COMMAND ----------

collection.delete_many({}) 

collection.insert_many(orders_list)

print("Data inserted successfully!")

# COMMAND ----------

graph_df = final_df.select("customer_id","product_id").dropDuplicates()

display(graph_df)

# COMMAND ----------

import pandas as pd 
graph_df = final_df.select("customer_id","product_id").dropDuplicates()

pdf = graph_df.toPandas()

pdf.to_csv("customer_product_graph.csv", index=False)

# COMMAND ----------

import os

os.listdir()


# COMMAND ----------

import shutil

shutil.copy("customer_product_graph.csv", "/databricks/driver/customer_product_graph.csv")

# COMMAND ----------

display(pdf)