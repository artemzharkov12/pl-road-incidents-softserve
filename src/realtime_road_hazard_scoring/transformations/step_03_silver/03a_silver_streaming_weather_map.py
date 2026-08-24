# Путь: src/transformations/03_silver/03a_silver_streaming_weather_map.py
from pyspark import pipelines as dp

# Импортируем нашу логику
from silver_logic import parse_weather_payload

dbutils.widgets.text("silver_schema", "artemzharkov10_silver")
SILVER_SCHEMA = dbutils.widgets.get("silver_schema")

@dp.table(
    name= f"{SILVER_SCHEMA}.silver_streaming_weather"
)
@dp.expect("valid_coordinates", "latitude >= 49.0 AND latitude <= 54.8") 
def silver_streaming_weather_data():
    bronze_df = dp.read_stream("bronze_streaming_weather")
    silver_df = parse_weather_payload(bronze_df)
    
    return silver_df