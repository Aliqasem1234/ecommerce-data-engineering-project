# Databricks notebook source
# MAGIC %md
# MAGIC Steg 1: Importera nödvändiga bibliotek
# MAGIC
# MAGIC I detta steg importeras relevanta Spark SQL-funktioner och datatyper.
# MAGIC Dessa behövs för att kunna utföra datatransformation, datarensning och schemahantering.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC Steg 2: Läs in rå kunddata
# MAGIC
# MAGIC Här läses den ursprungliga CSV-filen med kunddata in från Databricks Volume-lagringen till en Spark DataFrame.
# MAGIC Alternativet header=True säkerställer att kolumnnamn läses korrekt och inferSchema=True identifierar automatiskt rätt datatyper.
# MAGIC

# COMMAND ----------

customers_df = spark.read.option("header", True) \
    .option("inferSchema", True) \
    .csv("/Volumes/workspace/default/raw_data/customers.csv")

display(customers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Steg 3: Kontrollera datans struktur
# MAGIC
# MAGIC I detta steg granskas datasetets schema för att förstå kolumnnamn och datatyper innan rensningsprocessen påbörjas.

# COMMAND ----------

customers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Steg 4: Rensa bort inledande och avslutande mellanslag
# MAGIC
# MAGIC Här tas onödiga mellanslag bort från alla textkolumner.
# MAGIC Detta är viktigt för att undvika problem vid jämförelser och sammanfogningar (joins).

# COMMAND ----------

customers_df = customers_df.select(
    [trim(col(c)).alias(c) if t == "string" else col(c)
     for c, t in customers_df.dtypes]
)

display(customers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Steg 5: Standardisera land och e-post
# MAGIC
# MAGIC I detta steg normaliseras textfält genom att:
# MAGIC
# MAGIC Formatera landnamn med konsekvent stor begynnelsebokstav
# MAGIC
# MAGIC Konvertera e-postadresser till gemener
# MAGIC Detta säkerställer en enhetlig struktur i datan.

# COMMAND ----------

customers_df = customers_df.withColumn(
    "country",
    initcap(col("country"))
)

customers_df = customers_df.withColumn(
    "email",
    lower(col("email"))
)

display(customers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Steg 6: Normalisera registreringsdatum
# MAGIC
# MAGIC Registreringsdatum förekommer i flera olika format.
# MAGIC Här konverteras alla datum till ett enhetligt datumformat med hjälp av try_to_timestamp och coalesce.
# MAGIC Detta säkerställer konsekvent datumhantering i hela datasetet.

# COMMAND ----------

customers_df = customers_df.withColumn(
    "registration_date_clean",
    coalesce(
        to_date(expr("try_to_timestamp(registration_date, 'yyyy-MM-dd')")),
        to_date(expr("try_to_timestamp(registration_date, 'dd/MM/yyyy')")),
        to_date(expr("try_to_timestamp(registration_date, 'MM-dd-yyyy')")),
        to_date(expr("try_to_timestamp(registration_date, 'yyyy/MM/dd')")),
        to_date(expr("try_to_timestamp(registration_date, 'dd.MM.yyyy')"))
    )
)

display(customers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Steg 7: Filtrera bort poster utan kund-ID
# MAGIC
# MAGIC Rader där customer_id saknas tas bort för att säkerställa dataintegritet.
# MAGIC Varje kund måste ha ett unikt identifieringsnummer.

# COMMAND ----------

customers_df = customers_df.filter(col("customer_id").isNotNull())

display(customers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Steg 8: Eliminera dubbletter
# MAGIC
# MAGIC Eventuella duplicerade kundposter baserade på customer_id tas bort för att säkerställa att varje kund endast förekommer en gång.

# COMMAND ----------

customers_df=customers_df.dropDuplicates(['customer_id'])
display(customers_df)


# COMMAND ----------

# MAGIC %md
# MAGIC Steg 9: Ersätt originaldatumet med det rensade datumet
# MAGIC
# MAGIC Den ursprungliga datumkolumnen tas bort och ersätts med den standardiserade versionen för att bibehålla en konsekvent struktur.

# COMMAND ----------

customers_df = customers_df.drop("registration_date")

customers_df = customers_df.withColumnRenamed(
    "registration_date_clean",
    "registration_date"
)

display(customers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Steg 9: Ersätt originaldatumet med det rensade datumet
# MAGIC
# MAGIC Den ursprungliga datumkolumnen tas bort och ersätts med den standardiserade versionen för att bibehålla en konsekvent struktur.

# COMMAND ----------

customers_df.filter(col("registration_date").isNull()).count()

# COMMAND ----------

# MAGIC %md
# MAGIC Steg 11: Spara den rensade kundtabellen
# MAGIC
# MAGIC Den rensade kunddatan sparas som en hanterad Spark-tabell (clean_customers).
# MAGIC Tabellen används senare i integrations- och analysfasen.

# COMMAND ----------

customers_df.write.mode("overwrite").saveAsTable("clean_customers")