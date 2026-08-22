from pyspark import pipelines as dp
import pyspark.sql.functions as F

TEMP_WATER_FROST = int(spark.conf.get("TEMP_WATER_FROST", "-1"))
PRECIP_DRY_MAX = int(spark.conf.get("PRECIP_DRY_MAX", "0"))
PRECIP_LIGHT_MAX = int(spark.conf.get("PRECIP_LIGHT_MAX", "2"))


dbutils.widgets.text("gold_catalog", "dbr_dev")
dbutils.widgets.text("gold_schema", "artemzharkov10_gold")

GOLD_CATALOG = dbutils.widgets.get("gold_catalog")
GOLD_SCHEMA = dbutils.widgets.get("gold_schema")

GOLD_WEIGHTS_TABLE = f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_weather_cluster_weights"

@dp.table(name = f"{GOLD_SCHEMA}.gold_realtime_road_hazard")
def gold_realtime_road_hazard_data():
    
    df_silver = dp.read_stream("artemzharkov10_silver.silver_streaming_weather")
    df_clustered = (
        df_silver
        .withColumn(
            "temp_claster",
            F.when(F.col("soil_temperature_c") <= TEMP_WATER_FROST, "Frost")
            .when(F.col("soil_temperature_c") > TEMP_WATER_FROST, "Warm")
        )
        .withColumn(
            "precipitation_claster",
            F.when(F.col("precipitation_mm") == PRECIP_DRY_MAX, "Dry")
            .when((F.col("precipitation_mm") > PRECIP_DRY_MAX) & (F.col("precipitation_mm") <= PRECIP_LIGHT_MAX), "LightRain")
            .when(F.col("precipitation_mm") > PRECIP_LIGHT_MAX, "HeavyRain")
        )
        .withColumn(
            "weather_claster",
            F.concat_ws("_", F.col("temp_claster"), F.col("precipitation_claster"))
        )
    )

    df_weights = spark.table(GOLD_WEIGHTS_TABLE)

    df_final = (
        df_clustered.join(
            F.broadcast(df_weights),
            on=["weather_claster"],
            how="left"
        )
        .select(
            F.col("grid_id").alias("ID"),
            F.col("longitude"),
            F.col("latitude"),
            F.col("soil_temperature_c"),
            F.col("precipitation_mm"),
            F.col("weather_claster"),
            F.col("normalized_risk_index").alias("hazard_risk"),
            F.col("weather_time")
        )
    )
    return df_final
