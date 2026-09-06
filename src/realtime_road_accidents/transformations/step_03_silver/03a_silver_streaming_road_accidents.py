import pyspark.sql.functions as F
from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType 
from silver_logic_streaming_road_accidents import parse_entities_from_json

#Snapshot Polling implemantation

SILVER_SCHEMA = spark.conf.get("silver_schema", "artemzharkov10_silver")

silver_table_schema = StructType([
    StructField("accident_id", StringType(), True),
    StructField("icon_category", IntegerType(), True),
    StructField("impact_jams", IntegerType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("description", StringType(), True),
    StructField("raw_coordinates", StringType(), True),
    StructField("start_longitude", DoubleType(), True),
    StructField("start_latitude", DoubleType(), True),
    StructField("end_longitude", DoubleType(), True),
    StructField("end_latitude", DoubleType(), True),
    StructField("ingest_timestamp", TimestampType(), True),
    StructField("__START_AT", TimestampType(), False),
    StructField("__END_AT", TimestampType(), True)
])

# Example of json raw
#{"properties": {"id": "TTI-12a0622c-f076-45a7-868d-135acb5192b6-TTR39749638400015000", "iconCategory": 8, "magnitudeOfDelay": 4, "startTime": "2026-08-01T14:36:30Z", "events": [{"description": "Zamkniȩto"}]}, "geometry": {"coordinates": [[16.9572767236, 51.0788632966], [16.9566973664, 51.0779392744], [16.9565981247, 51.0777971438]]}}

basic_expectations = {
    "accident_id_not_null": "accident_id IS NOT NULL",
    "icon_category_not_null": "icon_category IS NOT NULL",
    "impact_jams_not_null": "impact_jams IS NOT NULL",
    "start_time_not_null": "start_time IS NOT NULL",
    "description_not_null": "description IS NOT NULL",
    "start_longitude_not_null" : "start_longitude IS NOT NULL",
    "start_latitude_not_null" : "start_latitude IS NOT NULL"
}

business_expectations = {
    "start_longitude_is_poland" : "start_longitude BETWEEN 14.1 AND 24.2", # two ways of implementation
    "start_latitude_is_poland" : "start_latitude >= 49.0 AND start_latitude <= 54.9",
    "icon_category_bigger_than_0": "icon_category >= 0"
}

# implementation virtual table in hash which allows us to avoid creating an additional table  
@dp.view(name = "silver_road_accidents_valid_view")
@dp.expect_all_or_drop(basic_expectations)
@dp.expect_all_or_drop(business_expectations)
def silver_road_accidents_valid_view():
    df = dp.read_stream("bronze_streaming_road_accidents")
    return parse_entities_from_json(df)

@dp.table(name = f"{SILVER_SCHEMA}.silver_streaming_road_accidents_quarantine")
def silver_streaming_road_accidents_quarantine():
    df = dp.read_stream("bronze_streaming_road_accidents") 
    parsed_df = parse_entities_from_json(df)

    # join all rule expectations
    invalid_conditions = []
    for rule in basic_expectations.values():
        invalid_conditions.append(f"NOT ({rule})")
    for rule in business_expectations.values():
        invalid_conditions.append(f"({rule}) IS NOT TRUE")
    final_invalid_condition = " OR ".join(invalid_conditions)   

    return parsed_df.filter(F.expr(final_invalid_condition))


dp.create_streaming_table(
    name = f"{SILVER_SCHEMA}.silver_streaming_road_accidents",
    schema = silver_table_schema
)
# Take data from View Table (micro-batch) than compare with existing rows and implament SCD2 if existing some changing or dedublication if some records are the same
#SCD2 ====
dp.apply_changes(
    target = f"{SILVER_SCHEMA}.silver_streaming_road_accidents",  # ????? when data transformation is did ?
    source = "silver_road_accidents_valid_view",
    keys=["accident_id"],
    sequence_by=F.col("ingest_timestamp"), # sort by time of enter in stream
    stored_as_scd_type="2",
    track_history_column_list=[
        "impact_jams", "description", 
        "raw_coordinates","start_longitude",
        "start_latitude","end_longitude","end_latitude"]
)


    
