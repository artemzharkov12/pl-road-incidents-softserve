from pyspark import pipelines as dp
import pyspark.sql.functions as F

SILVER_CATALOG = spark.conf.get("silver_catalog", "dbr_dev")
SILVER_SCHEMA = spark.conf.get("silver_schema", "artemzharkov10_silver")
SOURCE_TABLE = f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_streaming_road_accidents"
GOLD_SCHEMA = spark.conf.get("gold_schema", "artemzharkov10_gold")

@dp.table(name=f"{GOLD_SCHEMA}.gold_dim_event_type_streaming_road_accidents")
def create_dim_event_type():
    df = spark.table(SOURCE_TABLE)
    return (
        df.select("icon_category", "event_type_description", "impact_jams")
        .distinct()
        .withColumn(
            "event_type_key", 
            F.md5(F.concat_ws("||", F.col("icon_category"), F.col("event_type_description")))
        )
    )

@dp.table(name=f"{GOLD_SCHEMA}.gold_dim_participant_streaming_road_accidents")
def create_dim_participant():
    df = spark.table(SOURCE_TABLE)
    return (
        df.select("first_name", "last_name", "age", "gender")
        .distinct()
        .withColumn(
            "participant_key", 
            F.md5(F.concat_ws("||", F.col("first_name"), F.col("last_name"), F.col("age"), F.col("gender")))
        )
    )

@dp.table(name=f"{GOLD_SCHEMA}.gold_dim_time_streaming_road_accidents")
def create_dim_time():
    df = spark.table(SOURCE_TABLE)
    return (
        df.select(F.col("start_time").cast("timestamp").alias("datetime"))
        .filter(F.col("datetime").isNotNull())
        .distinct()
        .withColumn("date_key", F.date_format(F.col("datetime"), "yyyyMMddHH").cast("int"))
        .withColumn("date", F.to_date(F.col("datetime")))
        .withColumn("year", F.year(F.col("datetime")))
        .withColumn("month", F.month(F.col("datetime")))
        .withColumn("day", F.dayofmonth(F.col("datetime")))
        .withColumn("hour", F.hour(F.col("datetime")))
        .withColumn("day_of_week", F.dayofweek(F.col("datetime")))
    )