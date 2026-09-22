import sys
import os
import pytest
import pyspark.sql.functions as F
from pyspark.sql import Row
from databricks.connect import DatabricksSession

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from transformations.step_03_silver.silver_dq_rules import basic_expectations, business_expectations

@pytest.fixture(scope="module")
def spark():
    return DatabricksSession.builder.serverless().getOrCreate()

def test_dq_basic_expectations_drops_null_values(spark):
    data = [
        Row(accident_id="TTI-1", icon_category=1, impact_jams=1, start_time="2026-08-01", description="A", start_longitude=15.0, start_latitude=50.0), # Valid
        Row(accident_id=None, icon_category=1, impact_jams=1, start_time="2026-08-01", description="A", start_longitude=15.0, start_latitude=50.0)    # Invalid (NULL id)
    ]
    df = spark.createDataFrame(data)

    # Применяем правила как фильтры (симуляция поведения expect_all_or_drop)
    valid_df = df
    for rule in basic_expectations.values():
        valid_df = valid_df.filter(F.expr(rule))

    # Должна остаться только 1 валидная строка
    assert valid_df.count() == 1
    assert valid_df.collect()[0]["accident_id"] == "TTI-1"


def test_dq_business_expectations_filters_geography(spark):
    data = [
        Row(start_longitude=19.0, start_latitude=52.0, icon_category=1),  # Valid (Центр Польши)
        Row(start_longitude=10.0, start_latitude=52.0, icon_category=1),  # Invalid Longitude (Германия)
        Row(start_longitude=19.0, start_latitude=40.0, icon_category=1)   # Invalid Latitude (Юг Европы)
    ]
    df = spark.createDataFrame(data)

    valid_df = df
    for rule in business_expectations.values():
        valid_df = valid_df.filter(F.expr(rule))

    # Должна остаться только 1 валидная строка
    assert valid_df.count() == 1
    assert valid_df.collect()[0]["start_longitude"] == 19.0


def test_quarantine_logic_captures_invalid_records(spark):
    data = [
        Row(accident_id="TTI-1", start_longitude=19.0, start_latitude=52.0, icon_category=1), # Valid -> Not Quarantine
        Row(accident_id=None, start_longitude=19.0, start_latitude=52.0, icon_category=1),    # NULL id -> Quarantine
        Row(accident_id="TTI-3", start_longitude=10.0, start_latitude=52.0, icon_category=1)  # Bad Geo -> Quarantine
    ]
    
    # Заполнение отсутствующих колонок заглушками для теста
    full_data = [Row(**{**row.asDict(), "impact_jams": 1, "start_time": "2026", "description": "A"}) for row in data]
    df = spark.createDataFrame(full_data)

    # Генерация quarantine_condition (копируется из 03a_silver_streaming_road_accidents.py)
    invalid_conditions = []
    for rule in basic_expectations.values():
        invalid_conditions.append(f"NOT ({rule})")
    for rule in business_expectations.values():
        invalid_conditions.append(f"({rule}) IS NOT TRUE")
    final_invalid_condition = " OR ".join(invalid_conditions)

    quarantine_df = df.filter(F.expr(final_invalid_condition))
    quarantine_records = quarantine_df.collect()

    assert quarantine_df.count() == 2
    assert quarantine_records[0]["accident_id"] is None
    assert quarantine_records[1]["accident_id"] == "TTI-3"