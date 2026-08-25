import pyspark.sql.functions as F
from pyspark import pipelines as dp
from silver_logic import parse_weather_payload

dbutils.widgets.text("silver_schema", "artemzharkov10_silver")
SILVER_SCHEMA = dbutils.widgets.get("silver_schema")

rules = {
    "valid_coordinates": "latitude >= 49.0 AND latitude <= 54.8",
    "valid_precipitation": "precipitation_mm >= 0"
}

# Delta constrains level + data quality expectations
@dp.table(
    name=f"{SILVER_SCHEMA}.silver_streaming_weather",
    schema="""
        grid_id STRING NOT NULL,
        latitude DOUBLE,
        longitude DOUBLE,
        weather_time TIMESTAMP NOT NULL,
        precipitation_mm DOUBLE CONSTRAINT valid_precipitation CHECK (precipitation_mm >= 0),
        soil_temperature_c DOUBLE,
        eventhub_enqueued_time TIMESTAMP,
        ingest_timestamp TIMESTAMP
    """
)
@dp.expect_all_or_drop(rules)
def silver_streaming_weather_data():
    bronze_df = dp.read_stream("bronze_streaming_weather") # all transformation logic in silver_logic.py
    return parse_weather_payload(bronze_df)

# via filter(invalid_condition) system write just invalid records 
@dp.table(name=f"{SILVER_SCHEMA}.silver_streaming_weather_quarantine")
def silver_streaming_weather_quarantine():
    bronze_df = dp.read_stream("bronze_streaming_weather")
    parsed_df = parse_weather_payload(bronze_df)
    invalid_condition = f"NOT ({rules['valid_coordinates']} AND {rules['valid_precipitation']})"
    return parsed_df.filter(F.expr(invalid_condition))


