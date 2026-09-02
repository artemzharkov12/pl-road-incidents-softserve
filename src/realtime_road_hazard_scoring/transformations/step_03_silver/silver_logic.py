from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def get_weather_schema():
    return StructType([
        StructField("grid_id", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("time", StringType(), True),
        StructField("precipitation_mm", DoubleType(), True),
        StructField("soil_temperature_c", DoubleType(), True)
    ])


def parse_weather_payload(df):
    schema = get_weather_schema()
    return (
        df.withColumn("parsed_data", from_json(col("json_payload"), schema))
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