# Databricks notebook source
# MAGIC %md
# MAGIC Since a declarative approach was used in the lab, this code simply serves as a reminder of how the classic implementation method 
# MAGIC works.

# COMMAND ----------

from pyspark.sql.functions import col, explode
from pyspark.sql.types import IntegerType, DoubleType, TimestampType, DateType

# COMMAND ----------

dbutils.widgets.text("bronze_catalog","dbr_dev")
dbutils.widgets.text("bronze_schema", "artemzharkov10_bronze")

dbutils.widgets.text("silver_catalog","dbr_dev")
dbutils.widgets.text("silver_schema","artemzharkov10_silver")

BRONZE_CATALOG = dbutils.widgets.get("bronze_catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")

SILVER_CATALOG = dbutils.widgets.get("silver_catalog")
SILVER_SCHEMA = dbutils.widgets.get("silver_schema")


# COMMAND ----------


BRONZE_TABLE_PATH = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.bronze_sewik"
TARGET_ACCIDENTS = f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_sewik_accidents"
CHECKPOINT_ACCIDENTS = f"/Volumes/{SILVER_CATALOG}/{SILVER_SCHEMA}/checkpoints/silver_accidents"

# COMMAND ----------

bronze_df = spark.readStream.table(BRONZE_TABLE_PATH)

# COMMAND ----------

accidents_df = (
    bronze_df
    .filter("ID IS NOT NULL AND DATA_ZDARZENIA IS NOT NULL")
    .select(
        col("ID"),
        col("JEDNOSTKA_MIEJSCA"),
        col("JEDNOSTKA_LIKWIDUJACA"),
        col("JEDNOSTKA_OPERATORA"),
        col("GPS_X_GUS").cast(DoubleType()).alias("GPS_X_GUS"),
        col("GPS_Y_GUS").cast(DoubleType()).alias("GPS_Y_GUS"),
        col("WOJ"),
        col("GMINA"),
        col("POWIAT"),
        col("MIEJSCOWOSC"),
        col("DATA_ZDARZENIA").cast(TimestampType()).alias("DATA_ZDARZENIA"),
        col("DATA_ZDARZ").cast(DateType()).alias("DATA_ZDARZ"),
        col("GODZINA_ZDARZ"),
        col("WSP_GPS_X").cast(DoubleType()).alias("WSP_GPS_X"),
        col("WSP_GPS_Y").cast(DoubleType()).alias("WSP_GPS_Y"),
        col("PREDKOSC_DOPUSZCZALNA").cast(IntegerType()).alias("PREDKOSC_DOPUSZCZALNA"),
        col("DROGA_PUBLICZNA"),
        col("DATA_ZGLOSZENIA").cast(TimestampType()).alias("DATA_ZGLOSZENIA"),
        col("DATA_PRZYJAZDU").cast(TimestampType()).alias("DATA_PRZYJAZDU"),
        col("GEOD_KODY"),
        col("ZABU_KOD"),
        col("CHMZ_KOD"),
        col("SSWA_KOD")
    )
    .dropDuplicates(["ID"])
)

# COMMAND ----------

query_accidents = (
    accidents_df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_ACCIDENTS)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(TARGET_ACCIDENTS)
)
spark.streams.awaitAnyTermination()

# COMMAND ----------

# silver_table = spark.read.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_sewik_accidents")
# display(silver_table)