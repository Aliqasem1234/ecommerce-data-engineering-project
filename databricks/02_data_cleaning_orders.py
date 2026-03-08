# Databricks notebook source
# MAGIC %md
# MAGIC Steg 1: Importera nödvändiga bibliotek
# MAGIC
# MAGIC I detta steg importeras Spark SQL-funktioner som krävs för datatransformation och rensning av orderdata.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC Steg 2: Läs in rå orderdata
# MAGIC
# MAGIC Här laddas den ursprungliga orders CSV-filen in från Databricks Volume till en Spark DataFrame.
# MAGIC Schema identifieras automatiskt för att möjliggöra korrekt databehandling.

# COMMAND ----------

orders_df = spark.read.option("header", True) \
    .option("inferSchema", True) \
    .csv("/Volumes/workspace/default/raw_data/orders.csv")

display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Steg 3: Granska datastruktur
# MAGIC
# MAGIC I detta steg analyseras kolumnnamn och datatyper för att identifiera potentiella dataproblem innan rensning påbörjas.

# COMMAND ----------

orders_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Steg 4: Normalisera orderdatum
# MAGIC
# MAGIC Orderdatum kan förekomma i olika format.
# MAGIC Här konverteras samtliga datum till ett enhetligt timestamp-format med hjälp av try_to_timestamp och coalesce.
# MAGIC Detta säkerställer konsekvent tidsanalys.

# COMMAND ----------

from pyspark.sql.functions import col, trim, lower, coalesce

orders_df = orders_df.select(
    [trim(col(c)).alias(c) if t == "string" else col(c)
     for c, t in orders_df.dtypes]
)

display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Steg 5: Standardisera orderstatus
# MAGIC
# MAGIC Orderstatus konverteras till gemener (lowercase) för att säkerställa konsekvent värdehantering vid analys och gruppering.

# COMMAND ----------

orders_df = orders_df.withColumn(
    "status",
    lower(col("status"))
)

display(orders_df)

# COMMAND ----------

orders_df= orders_df.filter(col("customer_id").isNotNull())


# COMMAND ----------

orders_df = orders_df.dropDuplicates(["order_id"])

# COMMAND ----------

from pyspark.sql.functions import *


orders_df = orders_df.withColumn(
    "order_date_fixed",
    regexp_replace("order_date", "T", " ")
)


orders_df = orders_df.withColumn(
    "order_date_clean",
    coalesce(
        expr("try_to_timestamp(order_date_fixed, 'yyyy-MM-dd HH:mm:ss')"),
        expr("try_to_timestamp(order_date_fixed, 'MM-dd-yyyy HH:mm:ss')"),
        expr("try_to_timestamp(order_date_fixed, 'dd/MM/yyyy HH:mm')"),
        expr("try_to_timestamp(order_date_fixed, 'yyyy/MM/dd HH:mm:ss')")
    )
)


orders_df = orders_df.drop("order_date", "order_date_fixed")
orders_df = orders_df.withColumnRenamed("order_date_clean", "order_date")

display(orders_df)

# COMMAND ----------

orders_df.filter(col("total_amount") < 0).count()

# COMMAND ----------

orders_df.write.mode("overwrite").saveAsTable("clean_orders")