from pyspark import pipelines as dp
import pyspark.sql.functions as F

SILVER_CATALOG = spark.conf.get("silver_catalog", "dbr_dev")
SILVER_SCHEMA = spark.conf.get("silver_schema", "artemzharkov10_silver")
SOURCE_TABLE = f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_streaming_road_accidents"
GOLD_SCHEMA = spark.conf.get("gold_schema", "artemzharkov10_gold")

@dp.table(name=f"{GOLD_SCHEMA}.gold_fact_accidents_streaming_road_accidents")
def create_fact_accidents():
    df = spark.table(SOURCE_TABLE)
    df = df.withColumn(
        "event_type_key", 
        F.md5(F.concat_ws("||", F.col("icon_category"), F.col("event_type_description")))
    )
    df = df.withColumn(
        "participant_key", 
        F.md5(F.concat_ws("||", F.col("first_name"), F.col("last_name"), F.col("age"), F.col("gender")))
    )
    df = df.withColumn(
        "date_key", 
        F.date_format(F.col("start_time").cast("timestamp"), "yyyyMMddHH").cast("int")
    )
    return df.select(
        "accident_id",
        "date_key",
        "event_type_key",
        "participant_key",
        "start_longitude",
        "start_latitude",
        "end_longitude",
        "end_latitude",
        "description",
        "raw_coordinates"
    )