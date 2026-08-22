from pyspark import pipelines as dp
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


dbutils.widgets.text("silver_schema", "artemzharkov10_silver")
SILVER_SCHEMA = dbutils.widgets.get("silver_schema")

weather_schema = StructType([
    StructField("grid_id", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("time", StringType(), True),
    StructField("precipitation_mm", DoubleType(), True),
    StructField("soil_temperature_c", DoubleType(), True)
])

@dp.table(
    name= f"{SILVER_SCHEMA}.silver_streaming_weather"
)
@dp.expect("valid_coordinates", "latitude >= 49.0 AND latitude <= 54.8") 
def silver_streaming_weather_data():
    bronze_df = dp.read_stream("bronze_streaming_weather")
    silver_df = (
        bronze_df
        .withColumn("parsed_data", from_json(col("json_payload"), weather_schema))
        .select(
            col("parsed_data.grid_id").alias("grid_id"),
            col("parsed_data.latitude").alias("latitude"),
            col("parsed_data.longitude").alias("longitude"),
            to_timestamp(col("parsed_data.time")).alias("weather_time"),
            col("parsed_data.precipitation_mm").alias("precipitation_mm"),
            col("parsed_data.soil_temperature_c").alias("soil_temperature_c"),
            col("eventhub_enqueued_time"),
            col("ingest_timestamp")
        )
    )
    return silver_df

