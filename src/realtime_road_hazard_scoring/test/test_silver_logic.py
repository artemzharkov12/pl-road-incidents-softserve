# import sys
# import os
# import pytest
# from databricks.connect import DatabricksSession
# from transformations.step_03_silver.silver_logic import parse_weather_payload


# # exit from directory (test) to (realtime_road_hazard_scoring)
# current_dir = os.path.dirname(os.path.abspath(__file__))
# project_root = os.path.abspath(os.path.join(current_dir, ".."))
# if project_root not in sys.path:
#     sys.path.insert(0, project_root)

# @pytest.fixture(scope="session")
# def spark():
#     return DatabricksSession.builder.serverless().getOrCreate()

# def test_parse_weather_payload(spark):
#     fake_json = '{"grid_id": "G1", "latitude": 52.0, "longitude": 21.0, "time": "2026-08-23 10:00:00", "precipitation_mm": 2.5, "soil_temperature_c": 15.0}'

#     bronze_df = spark.createDataFrame([
#         (fake_json, "2026-08-23T10:01:00Z", "2026-08-23T10:02:00Z")
#     ], ["json_payload", "eventhub_enqueued_time", "ingest_timestamp"])
    
#     result_df = parse_weather_payload(bronze_df)
#     result = result_df.collect()[0]

#     assert result["grid_id"] == "G1"
#     assert result["precipitation_mm"] == 2.5
#     assert result["latitude"] == 52.0



import sys
import os
import pytest
from databricks.connect import DatabricksSession

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from transformations.step_03_silver.silver_logic import parse_weather_payload

@pytest.fixture(scope="session")
def spark():
    return DatabricksSession.builder.remote(
        host="https://dbc-6d6709e1-4e5d.cloud.databricks.com/", 
        token="dapi538a5e9d6db1b7e30bfda0001f0f154a"
    ).serverless().getOrCreate()

def test_parse_weather_payload(spark):
    fake_json = '{"grid_id": "G1", "latitude": 52.0, "longitude": 21.0, "time": "2026-08-23 10:00:00", "precipitation_mm": 2.5, "soil_temperature_c": 15.0}'
    
    bronze_df = spark.createDataFrame([
        (fake_json, "2026-08-23T10:01:00Z", "2026-08-23T10:02:00Z")
    ], ["json_payload", "eventhub_enqueued_time", "ingest_timestamp"])
    
    result_df = parse_weather_payload(bronze_df)
    result = result_df.collect()[0]
    
    assert result["grid_id"] == "G1"
    assert result["precipitation_mm"] == 2.5
    assert result["latitude"] == 52.0
