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
TARGET_PARTICIPANTS = f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_sewik_participants"
CHECKPOINT_PARTICIPANTS = f"/Volumes/{SILVER_CATALOG}/{SILVER_SCHEMA}/checkpoints/silver_participants"

# COMMAND ----------

bronze_df = spark.readStream.table(BRONZE_TABLE_PATH)

# COMMAND ----------

participants_df = (
    bronze_df
    .select(
        col("ID").alias("ZDARZENIE_ID"),
        explode(col("UCZESTNICY")).alias("uczestnik")
    )
    .select(
        col("ZDARZENIE_ID"),
        col("uczestnik.ID").alias("UCZESTNIK_ID"),
        col("uczestnik.PLEC").alias("PLEC"),
        col("uczestnik.LICZBA_LAT_KIEROWANIA").cast(IntegerType()).alias("LICZBA_LAT_KIEROWANIA"),
        col("uczestnik.POZIOM_ALKOHOLU").cast(DoubleType()).alias("POZIOM_ALKOHOLU")
    )
    .filter("UCZESTNIK_ID IS NOT NULL") 
)

# COMMAND ----------

query_participants = (
    participants_df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PARTICIPANTS)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(TARGET_PARTICIPANTS)
)
spark.streams.awaitAnyTermination()

# COMMAND ----------

# silver_table = spark.read.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_sewik_participants")
# display(silver_table)