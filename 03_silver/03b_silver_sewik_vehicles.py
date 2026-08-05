# Databricks notebook source
# MAGIC %md
# MAGIC Since a declarative approach was used in the lab, this code simply serves as a reminder of how the classic implementation method works.

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
TARGET_VEHICLES = f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_sewik_vehicles"
CHECKPOINT_VEHICLES = f"/Volumes/{SILVER_CATALOG}/{SILVER_SCHEMA}/checkpoints/silver_vehicles"

# COMMAND ----------

bronze_df = spark.readStream.table(BRONZE_TABLE_PATH)

# COMMAND ----------

vehicles_df = (
    bronze_df
    .select(
        col("ID").alias("ZDARZENIE_ID"),
        explode(col("POJAZDY")).alias("pojazd")
    )
    .select(
        col("ZDARZENIE_ID"),
        col("pojazd.ID").alias("POJAZD_ID"),
        col("pojazd.RODZAJ_POJAZDU").alias("RODZAJ_POJAZDU"),
        col("pojazd.MARKA").alias("MARKA"),
        col("pojazd.ROK_PRODUKCJI").cast(IntegerType()).alias("ROK_PRODUKCJI")
    )
    .filter("POJAZD_ID IS NOT NULL")
)

# COMMAND ----------

query_vehicles = (
    vehicles_df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_VEHICLES)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(TARGET_VEHICLES)
)
spark.streams.awaitAnyTermination()

# COMMAND ----------

# silver_table = spark.read.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_sewik_vehicles")
# display(silver_table)