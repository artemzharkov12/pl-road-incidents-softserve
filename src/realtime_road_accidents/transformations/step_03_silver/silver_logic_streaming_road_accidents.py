import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, ArrayType

def get_schema():
    # we do not add a geomentry field for avoiding a missing data because we can obtain dict or just array of coordinates (depending on type of accident)
    return(
        StructType([
            StructField("properties", StructType([
                StructField("id", StringType(), True),
                StructField("iconCategory", IntegerType(), True),
                StructField("magnitudeOfDelay", IntegerType(), True),
                StructField("startTime", StringType(), True),
                StructField("events", ArrayType(StructType([
                    StructField("description", StringType(), True)
                ])), True)
            ]), True)
        ])
    )

def parse_entities_from_json(df):
    schema = get_schema()
    return(
        # transform our constant columns
        df.withColumn("json", F.from_json(F.col("json_payload"), schema))
        # transform our dynamic columns
        .withColumn("raw_coords_text", F.get_json_object(F.col("json_payload"), "$.geometry.coordinates"))
        .withColumn("clean_coords", F.regexp_replace(F.col("raw_coords_text"), r"[\[\]]", ""))
        .withColumn("coords_array", F.split(F.col("clean_coords"), ","))
        .withColumn("coords_size", F.size(F.col("coords_array")))
        
        .select(
            F.col("json.properties.id").alias("accident_id"),
            F.col("json.properties.iconCategory").alias("icon_category"),
            F.col("json.properties.magnitudeOfDelay").alias("impact_jams"),
            F.to_timestamp(F.col("json.properties.startTime"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("start_time"),
            F.concat_ws(", ", F.col("json.properties.events.description")).alias("description"),
            # To normalize the data, we take the first and last coordinates of the incident.
            F.when(F.col("clean_coords") != "", F.col("coords_array").getItem(0).cast("double")).otherwise(None).alias("start_longitude"),
            F.when(F.col("clean_coords") != "", F.col("coords_array").getItem(1).cast("double")).otherwise(None).alias("start_latitude"),
            F.when(F.col("coords_size") > 2, F.element_at(F.col("coords_array"), -2).cast("double")).otherwise(None).alias("end_longitude"),
            F.when(F.col("coords_size") > 2, F.element_at(F.col("coords_array"), -1).cast("double")).otherwise(None).alias("end_latitude"),
            # write all coordinates as a string
            F.col("raw_coords_text").alias("raw_coordinates"),
            F.current_timestamp().alias("ingest_timestamp")
        )
        # Business logic for Lab 10: Lakehouse Federation & CDC
        .withColumn("event_type_description",
            F.when(F.col("icon_category") == 1, "Accident")
             .when(F.col("icon_category") == 6, "Traffic Jam")
             .when(F.col("icon_category") == 7, "Hazard")
             .when(F.col("icon_category") == 8, "Closed Road")
             .when(F.col("icon_category") == 9, "Road Works")
             .when(F.col("icon_category") == 11, "Fog")
             .otherwise("Other/Unknown")
        )
        .withColumn("is_deleted", F.when(F.col("icon_category") == 0, True).otherwise(False))
        .withColumn("fk_driver_id",
            F.when(F.col("icon_category") == 1, (F.abs(F.hash(F.col("accident_id"))) % 100 + 1).cast("int"))
             .otherwise(F.lit(None).cast("int"))
        )
    )