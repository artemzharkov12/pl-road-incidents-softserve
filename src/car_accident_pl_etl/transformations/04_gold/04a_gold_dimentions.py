from pyspark import pipelines as dp
from pyspark.sql import functions as F


# GOLD_CATALOG = spark.conf.get("gold_catalog", "dbr_dev")
# GOLD_SCHEMA = spark.conf.get("gold_schema", "artemzharkov10_gold")

# execute dim_date dimention table =======================
@dp.table(name = "gold_dim_date")
def create_gold_dim_date():
    import holidays

    SILVER_CATALOG = spark.conf.get("silver_catalog", "dbr_dev")
    SILVER_SCHEMA = spark.conf.get("silver_schema", "artemzharkov10_silver")    

    df_silver_accidents = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_sewik_accidents")
    # python library for all holidays in Poland
    years_range = range(2018, 2024)
    pl_holidays = holidays.Poland(years=years_range)
    holiday_dates_list = [str(date) for date in pl_holidays.keys()] # return YYYY-MM-DD

    df_dim_date = df_silver_accidents.select(F.col("accident_date").alias("full_date")).distinct()
    df_dim_date = (
            df_dim_date
                .withColumn("date_id", F.date_format(F.col("full_date"),"yyyyMMdd").cast("int"))  # init PK
                .withColumn("month", F.month("full_date").cast("int")) # created for determinating season period
                .withColumn("season", F.when(F.col("month").isin(12,1,2), "Winter")
                            .when(F.col("month").isin(3,4,5), "Spring")
                            .when(F.col("month").isin(6,7,8), "Summer")
                            .when(F.col("month").isin(9,10,11), "Autumn")
                            .otherwise("Null"))
                .withColumn("is_holiday", F.when(F.col("full_date").cast("string").isin(holiday_dates_list),True).otherwise(False))
                .drop("month")
        )
    return df_dim_date
    # df_dim_date.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_dim_date")


# execute dim_location dimention table =======================
@dp.table(name = "gold_dim_location")
def create_gold_dim_location():

    SILVER_CATALOG = spark.conf.get("silver_catalog", "dbr_dev")
    SILVER_SCHEMA = spark.conf.get("silver_schema", "artemzharkov10_silver")

    df_silver_accidents = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_sewik_accidents")

    df_dim_location = df_silver_accidents.select(
        F.col("voivodeship"),
        F.col("municipality"),
        F.col("district"),
        F.col("city")).distinct() #  64000+ unique rows
    df_dim_location = (
        df_dim_location
        .withColumn("location_id",
                    F.md5(F.concat_ws("",
                        F.col("voivodeship"),
                        F.col("municipality"),
                        F.col("district"),
                        F.col("city")))) # PK like a hash 
    )
    return df_dim_location
    # df_dim_location.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_dim_location")


# execute dim_vechicle dimention table =======================
@dp.table(name = "gold_dim_vehicle")
def create_gold_dim_vehicle():

    SILVER_CATALOG = spark.conf.get("silver_catalog", "dbr_dev")
    SILVER_SCHEMA = spark.conf.get("silver_schema", "artemzharkov10_silver")

    df_silver_vehicles = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_sewik_vehicles")
    
    df_vehicles = df_silver_vehicles.select(
        F.col("vehicle_id"),
        F.col("vehicle_type"),
        F.col("brand")
    ).distinct()
    return df_vehicles
    # df_vehicles.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_dim_vehicle")


# execute dim_participant dimention table =======================
@dp.table(name = "gold_dim_participant")
def create_gold_dim_participant():

    SILVER_CATALOG = spark.conf.get("silver_catalog", "dbr_dev")
    SILVER_SCHEMA = spark.conf.get("silver_schema", "artemzharkov10_silver")

    df_silver_participants = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_sewik_participants")

    dim_participants = df_silver_participants.select(
        F.col("participant_id"), 
        F.col("gender"),
        F.col("driving_experience_years")).distinct()
    return dim_participants
    # dim_participants.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_dim_participant")




