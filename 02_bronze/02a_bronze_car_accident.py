# Databricks notebook source
# MAGIC %md
# MAGIC Since a declarative approach was used in the lab, this code simply serves as a reminder of how the classic implementation method works.

# COMMAND ----------

from pyspark import pipelines
from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

dbutils.widgets.text("bronze_catalog","dbr_dev")
dbutils.widgets.text("bronze_schema", "artemzharkov10_bronze")

CATALOG = dbutils.widgets.get("bronze_catalog")
SCHEMA = dbutils.widgets.get("bronze_schema")


# COMMAND ----------

SOURCE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data/"

# COMMAND ----------

bronze_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format","json")
    .option("cloudFiles.schemaLocation", CHECKPOINT_PATH)
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .load(SOURCE_PATH)
    .withColumn("source_filename", col("_metadata.file_path"))
    .withColumn("ingest_timestamp", current_timestamp())
)

# COMMAND ----------

query = (bronze_df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .option("mergeSchema", "true")
    .toTable(TABLE_NAME)
)
query.awaitTermination()

# COMMAND ----------

# # bronze_table = spark.read.table(f"{CATALOG}.{SCHEMA}.sewik_bronze")
# # display(bronze_table)
# total_rows = spark.table(TABLE_NAME).count()
# print(f"{total_rows}")