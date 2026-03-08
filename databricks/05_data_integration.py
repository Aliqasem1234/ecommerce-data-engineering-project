# Databricks notebook source
from pyspark.sql.functions import *

customers_df = spark.table("clean_customers")
orders_df = spark.table("clean_orders")
order_items_df = spark.table("clean_order_items")
products_df = spark.table("clean_products")

# COMMAND ----------

orders_customers_df = orders_df.join(
    customers_df,
    on="customer_id",
    how="inner"
)

display(orders_customers_df)

# COMMAND ----------

orders_full_df = orders_customers_df.join(
    order_items_df,
    on="order_id",
    how="inner"
)

display(orders_full_df)

# COMMAND ----------

final_df = orders_full_df.join(
    products_df,
    on="product_id",
    how="inner"
)

display(final_df)

# COMMAND ----------

final_df = final_df.withColumn(
    "revenue",
    col("quantity") * col("unit_price")
)

display(final_df)

# COMMAND ----------

print("Row count:", final_df.count())