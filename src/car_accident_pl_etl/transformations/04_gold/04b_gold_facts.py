from pyspark import pipelines as dp
from pyspark.sql import functions as F


GOLD_CATALOG = spark.conf.get("gold_catalog", "dbr_dev")
GOLD_SCHEMA = spark.conf.get("gold_schema", "artemzharkov10_gold")
SILVER_CATALOG = spark.conf.get("silver_catalog", "dbr_dev")
SILVER_SCHEMA = spark.conf.get("silver_schema", "artemzharkov10_silver")


#  fact_accident_summary ===========
@dp.table(
    name="gold_fact_accident_summary",
    catalog=GOLD_CATALOG,
    schema=GOLD_SCHEMA
)
def create_gold_fact_accident_summary():
    df_silver_accidents = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_sewik_accidents")
    df_silver_participants = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_sewik_participants")

    df_injured_counts = (df_silver_participants 
        .groupBy("accident_id") 
        .agg(F.count("participant_id").alias("injured_count"))
    ) # aggregating number of injuries per accident (like common table expresion)

    fact_accident_summary = (
        df_silver_accidents
        .join(df_injured_counts, on="accident_id", how="left")
        .select(
            F.col("accident_id"), #PK
            F.date_format(F.col("accident_date"),"yyyyMMdd").cast("int").alias("date_id"), # dim_date
            F.col("accident_timestamp"),
            F.md5(
                F.concat_ws("",
                    F.col("voivodeship"),
                    F.col("municipality"),
                    F.col("district"),
                    F.col("city"))).alias("location_id"), # dim_location
            F.col("gps_x_gus").alias("gps_x"),
            F.col("gps_y_gus").alias("gps_y"),
            F.coalesce(F.col("injured_count"), F.lit(0)).cast("int").alias("total_injured")
        )
    )
    return fact_accident_summary


#  fact_accident_details ==========
@dp.table(
    name="gold_fact_accident_details",
    catalog=GOLD_CATALOG,
    schema=GOLD_SCHEMA
)
def create_gold_fact_accident_details():
    df_silver_accidents = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_sewik_accidents")
    df_silver_vehicles = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_sewik_vehicles")
    df_silver_participants = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_sewik_participants")
    
    fact_accident_details = (
        df_silver_accidents
        .join(df_silver_vehicles,"accident_id")
        .join(df_silver_participants, "accident_id")
        .select(
            F.col("accident_id"),
            F.col("vehicle_id"),
            F.col("participant_id"),
            F.col("blood_alcohol_level")
            )
        .withColumn("blood_alcohol_level", F.coalesce(F.col("blood_alcohol_level").cast("double"), F.lit(0.0)))
        .withColumn("is_alcohol",F.when(F.col("blood_alcohol_level") > 0.05, True).otherwise(False))
    )
    return fact_accident_details