import pyspark.sql.functions as F
from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType 
# Business logic
from silver_logic_streaming_road_accidents import parse_entities_from_json
# Data quality sets
from silver_dq_rules import basic_expectations, business_expectations

SILVER_SCHEMA = spark.conf.get("silver_schema", "artemzharkov10_silver")

silver_table_schema = StructType([
    StructField("accident_id", StringType(), True),
    StructField("icon_category", IntegerType(), True),
    StructField("event_type_description", StringType(), True),
    StructField("impact_jams", IntegerType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("description", StringType(), True),
    StructField("raw_coordinates", StringType(), True),
    StructField("start_longitude", DoubleType(), True),
    StructField("start_latitude", DoubleType(), True),
    StructField("end_longitude", DoubleType(), True),
    StructField("end_latitude", DoubleType(), True),
    StructField("ingest_timestamp", TimestampType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("is_deleted", BooleanType(), True),
    StructField("__START_AT", TimestampType(), False),
    StructField("__END_AT", TimestampType(), True)
])

@dp.view(name = "silver_road_accidents_valid_view")
@dp.expect_all_or_drop(basic_expectations)
@dp.expect_all_or_drop(business_expectations)
def silver_road_accidents_valid_view():
    df = dp.read_stream("bronze_streaming_road_accidents")
    parsed_df = parse_entities_from_json(df)
    
    drivers_df = spark.table("lab10_catalog.public.external_drivers")
    
    enriched_df = parsed_df.join(
        F.broadcast(drivers_df), # avoid shuffle
        parsed_df.fk_driver_id == drivers_df.driver_id,
        "left"
    ).drop("fk_driver_id", "driver_id")
    
    return enriched_df

@dp.table(name = f"{SILVER_SCHEMA}.silver_streaming_road_accidents_quarantine")
def silver_streaming_road_accidents_quarantine():
    df = dp.read_stream("bronze_streaming_road_accidents") 
    parsed_df = parse_entities_from_json(df)

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

dp.apply_changes(
    target = f"{SILVER_SCHEMA}.silver_streaming_road_accidents",
    source = "silver_road_accidents_valid_view",
    keys=["accident_id"],
    sequence_by=F.col("ingest_timestamp"),
    apply_as_deletes=F.expr("is_deleted = true"),
    stored_as_scd_type="2",
    track_history_column_list=[
        "event_type_description", "impact_jams", "description", 
        "raw_coordinates", "start_longitude", "start_latitude", 
        "end_longitude", "end_latitude", "first_name", "last_name", 
        "age", "gender"
    ]
)