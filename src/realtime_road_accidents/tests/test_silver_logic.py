import sys
import os
import pytest
from datetime import datetime
from pyspark.sql.types import TimestampType
from databricks.connect import DatabricksSession

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from silver_logic_streaming_road_accidents import parse_entities_from_json

@pytest.fixture(scope="session")
def spark():
    return DatabricksSession.builder.serverless().getOrCreate()

def test_parse_entities_from_json_valid_data(spark):
    fake_json = '{"properties": {"id": "TTI-12345", "iconCategory": 8, "magnitudeOfDelay": 4, "startTime": "2026-08-01T14:36:30Z", "events": [{"description": "Zamknieto"}]}, "geometry": {"coordinates": [[16.95, 51.07], [16.96, 51.08], [16.97, 51.09]]}}'
    
    bronze_df = spark.createDataFrame([
        (fake_json,)
    ], ["json_payload"])
    
    result_df = parse_entities_from_json(bronze_df)
    result = result_df.collect()[0]
    assert result["accident_id"] == "TTI-12345"
    assert result["icon_category"] == 8
    assert result["impact_jams"] == 4
    assert result["start_time"] == datetime(2026, 8, 1, 14, 36, 30)
    assert result["description"] == "Zamknieto"
    assert result["start_longitude"] == 16.95
    assert result["start_latitude"] == 51.07
    assert result["end_longitude"] == 16.97
    assert result["end_latitude"] == 51.09
    assert isinstance(result_df.schema["start_time"].dataType, TimestampType)
    assert result["ingest_timestamp"] is not None

def test_parse_entities_from_json_empty_coordinates(spark):
    fake_json = '{"properties": {"id": "TTI-99999", "iconCategory": 0, "magnitudeOfDelay": 0, "startTime": "2026-08-01T14:36:30Z", "events": []}, "geometry": {"coordinates": []}}'
    
    bronze_df = spark.createDataFrame([
        (fake_json,)
    ], ["json_payload"])
    
    result_df = parse_entities_from_json(bronze_df)
    result = result_df.collect()[0]
    assert result["accident_id"] == "TTI-99999"
    assert result["start_longitude"] is None
    assert result["start_latitude"] is None
    assert result["end_longitude"] is None
    assert result["end_latitude"] is None


def test_parse_entities_from_json_single_coordinate(spark):
    fake_json = '{"properties": {"id": "TTI-222", "iconCategory": 1, "magnitudeOfDelay": 0, "startTime": "2026-08-01T15:00:00Z", "events": [{"description": "Wypadek"}]}, "geometry": {"coordinates": [[21.0122, 52.2297]]}}'
    
    bronze_df = spark.createDataFrame([(fake_json,)], ["json_payload"])
    result = parse_entities_from_json(bronze_df).collect()[0]

    assert result["start_longitude"] == 21.0122
    assert result["start_latitude"] == 52.2297
    assert result["end_longitude"] is None
    assert result["end_latitude"] is None


def test_parse_entities_from_json_multiple_events(spark):
    fake_json = '{"properties": {"id": "TTI-333", "iconCategory": 3, "magnitudeOfDelay": 2, "startTime": "2026-08-01T16:00:00Z", "events": [{"description": "Roboty drogowe"}, {"description": "Zwezenie jezdni"}]}, "geometry": {"coordinates": []}}'
    
    bronze_df = spark.createDataFrame([(fake_json,)], ["json_payload"])
    result = parse_entities_from_json(bronze_df).collect()[0]

    assert result["description"] == "Roboty drogowe, Zwezenie jezdni"