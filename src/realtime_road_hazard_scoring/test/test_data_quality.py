import os
import pytest
import pyspark.sql.functions as F
from databricks.connect import DatabricksSession


BRONZE_SCHEMA = os.getenv("BRONZE_SCHEMA", "artemzharkov10_bronze")
SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "artemzharkov10_silver")
GOLD_SCHEMA = os.getenv("GOLD_SCHEMA", "artemzharkov10_gold")

@pytest.fixture(scope="session")
def spark():
    return DatabricksSession.builder.getOrCreate()

def test_layer_reconciliation(spark):
    bronze_count = spark.table(f"{BRONZE_SCHEMA}.bronze_streaming_weather").count()
    silver_valid_count = spark.table(f"{SILVER_SCHEMA}.silver_streaming_weather").count()
    silver_quarantine_count = spark.table(f"{SILVER_SCHEMA}.silver_streaming_weather_quarantine").count()
    
    assert bronze_count == (silver_valid_count + silver_quarantine_count), \
        f"Error Bronze({bronze_count}) != Silver({silver_valid_count}) + Quarantine({silver_quarantine_count})"

    silver_precip = spark.table(f"{SILVER_SCHEMA}.silver_streaming_weather") \
        .select(F.sum("precipitation_mm")).collect()[0][0] or 0.0
        
    gold_precip = spark.table(f"{GOLD_SCHEMA}.gold_weather_summary") \
        .select(F.sum("total_precipitation_mm")).collect()[0][0] or 0.0

    assert abs(silver_precip - gold_precip) < 0.01, \
        f"Agreg error: Silver({silver_precip}) != Gold({gold_precip})"



