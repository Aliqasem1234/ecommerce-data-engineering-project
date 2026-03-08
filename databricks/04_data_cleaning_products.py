# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

products_df = spark.read.option("header", True)\
    .option("inferSchema", True)\
    .csv("/Volumes/workspace/default/raw_data/products.csv")

display(products_df)

# COMMAND ----------

products_df = products_df.toDF(
    *[c.lower().strip() for c in products_df.columns]
)

display(products_df)

# COMMAND ----------

products_df.printSchema()

# COMMAND ----------

products_df = products_df.filter(
    (col("price") > 0)
)

display(products_df)

# COMMAND ----------

products_df = products_df.withColumn(
    "product_name",
    trim(col("product_name"))
)

display(products_df)

# COMMAND ----------

products_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in products_df.columns
]).show()

# COMMAND ----------

products_df.write.mode("overwrite").saveAsTable("clean_products")

# COMMAND ----------

