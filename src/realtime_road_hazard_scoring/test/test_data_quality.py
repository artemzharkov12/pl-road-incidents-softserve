import pyspark.sql.functions as F

dbutils.widgets.text("bronze_catalog", "dbr_dev")
dbutils.widgets.text("bronze_schema", "artemzharkov10_bronze")
BRONZE_CATALOG = dbutils.widgets.get("bronze_catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")

dbutils.widgets.text("silver_catalog", "dbr_dev")
dbutils.widgets.text("silver_schema", "artemzharkov10_silver")
SILVER_CATALOG = dbutils.widgets.get("silver_catalog")
SILVER_SCHEMA = dbutils.widgets.get("silver_schema")

dbutils.widgets.text("gold_catalog", "dbr_dev")
dbutils.widgets.text("gold_schema", "artemzharkov10_gold")
GOLD_CATALOG = dbutils.widgets.get("gold_catalog")
GOLD_SCHEMA = dbutils.widgets.get("gold_schema")

bronze_count = spark.table(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.bronze_streaming_weather").count()
silver_valid_count = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_streaming_weather").count()
silver_quarantine_count = spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_streaming_weather_quarantine").count()

assert bronze_count == (silver_valid_count + silver_quarantine_count),\
f"Error Bronze({bronze_count}) != Silver({silver_valid_count}) + Quarantine({silver_quarantine_count})"

# Aggregate checks
silver_precip = (
    spark.table(f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_streaming_weather") 
    .select(F.sum("precipitation_mm")).collect()[0][0])

gold_precip = (
    spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_realtime_road_hazard") 
    .select(F.sum("precipitation_mm")).collect()[0][0])

assert abs(silver_precip - gold_precip) < 0.01, f"Agreg error: Silver({silver_precip}) != Gold({gold_precip})"





