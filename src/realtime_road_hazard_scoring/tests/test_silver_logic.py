import sys
import os
import pytest
from pyspark.sql.types import TimestampType
from databricks.connect import DatabricksSession

# go to the global directory
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from transformations.step_03_silver.silver_logic import parse_weather_payload

@pytest.fixture(scope="session")
def spark():
    return DatabricksSession.builder.serverless().getOrCreate()

def test_parse_weather_payload(spark):
    fake_json = '{"grid_id": "G1", "latitude": 52.0, "longitude": 21.0, "time": "2026-08-23 10:00:00", "precipitation_mm": 2.5, "soil_temperature_c": 15.0}'
    
    bronze_df = spark.createDataFrame([
        (fake_json, "2026-08-23T10:01:00Z", "2026-08-23T10:02:00Z")
    ], ["json_payload", "eventhub_enqueued_time", "ingest_timestamp"])
    
    result_df = parse_weather_payload(bronze_df)
    result = result_df.collect()[0]
    
    assert result["grid_id"] == "G1"
    assert result["latitude"] == 52.0
    assert result["longitude"] == 21.0
    assert result["precipitation_mm"] == 2.5
    assert result["soil_temperature_c"] == 15.0
    assert result["weather_time"] is not None
    assert isinstance(result_df.schema["weather_time"].dataType, TimestampType)