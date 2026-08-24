import sys
import os
import pytest

# 1. Помогаем Python найти соседние папки.
# Берем путь к текущей папке (test) и поднимаемся на уровень вверх (в realtime_road_hazard_scoring)
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, ".."))

if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 2. ТЕПЕРЬ ИМПОРТ ПРАВИЛЬНЫЙ!
# Указываем полный путь: transformations -> step_03_silver -> silver_logic
from transformations.step_03_silver.silver_logic import parse_weather_payload

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    # Запускаем локальный Spark внутри контейнера для тестирования функции
    return SparkSession.builder \
        .appName("UnitTests") \
        .master("local[*]") \
        .getOrCreate()
def test_parse_weather_payload(spark):
    # Фейковые данные
    fake_json = '{"grid_id": "G1", "latitude": 52.0, "longitude": 21.0, "time": "2026-08-23 10:00:00", "precipitation_mm": 2.5, "soil_temperature_c": 15.0}'
    
    # Имитация таблицы из Databricks
    bronze_df = spark.createDataFrame([
        (fake_json, "2026-08-23T10:01:00Z", "2026-08-23T10:02:00Z")
    ], ["json_payload", "eventhub_enqueued_time", "ingest_timestamp"])
    
    # Прогоняем логику
    result_df = parse_weather_payload(bronze_df)
    result = result_df.collect()[0]
    
    # Проверки
    assert result["grid_id"] == "G1"
    assert result["precipitation_mm"] == 2.5
    assert result["latitude"] == 52.0