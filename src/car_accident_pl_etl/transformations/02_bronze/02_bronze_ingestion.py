import dlt
from pyspark.sql.functions import current_timestamp, col

BRONZE_CATALOG = spark.conf.get("bronze_catalog", "dbr_dev")
BRONZE_SCHEMA = spark.conf.get("bronze_schema", "artemzharkov10_bronze")

SOURCE_PATH = f"/Volumes/{BRONZE_CATALOG}/{BRONZE_SCHEMA}/raw_data/"

@dlt.table(name="bronze_sewik")
def create_bronze_table():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .load(SOURCE_PATH)
        .withColumn("source_filename", col("_metadata.file_path"))
        .withColumn("ingest_timestamp", current_timestamp())
    )