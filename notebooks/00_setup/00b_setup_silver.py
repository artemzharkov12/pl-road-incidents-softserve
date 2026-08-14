# Databricks notebook source
dbutils.widgets.text("silver_catalog","dbr_dev")
dbutils.widgets.text("silver_schema","artemzharkov10_silver")

SILVER_CATALOG = dbutils.widgets.get("silver_catalog")
SILVER_SCHEMA = dbutils.widgets.get("silver_schema")

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_CATALOG}.{SILVER_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {SILVER_CATALOG}.{SILVER_SCHEMA}.checkpoints")


# COMMAND ----------

# MAGIC %md
# MAGIC Since a declarative approach was used in the lab, this code simply serves as a reminder of how the classic implementation method works.

# COMMAND ----------

# spark.sql(f"""
#     CREATE TABLE IF NOT EXISTS {SILVER_CATALOG}.{SILVER_SCHEMA}.silver_sewik_accidents (
#         accident_id STRING,
#         police_unit_local STRING,
#         police_unit_handling STRING,
#         police_unit_operator STRING,
#         gps_x_gus DOUBLE,
#         gps_y_gus DOUBLE,
#         voivodeship STRING,
#         municipality STRING,
#         district STRING,
#         city STRING,
#         accident_timestamp TIMESTAMP,
#         accident_date DATE,
#         accident_time STRING,
#         gps_x DOUBLE,
#         gps_y DOUBLE,
#         speed_limit INT,
#         public_road STRING,
#         report_timestamp TIMESTAMP,
#         arrival_timestamp TIMESTAMP,
#         geod_code ARRAY<STRING>,
#         built_up_area_code STRING,
#         light_conditions_code STRING,
#         traffic_lights_code STRING
#     )
# """)

# spark.sql(f"""
#     CREATE TABLE IF NOT EXISTS {SILVER_CATALOG}.{SILVER_SCHEMA}.silver_sewik_vehicles (
#         accident_id STRING,
#         vehicle_id STRING,
#         vehicle_type STRING,
#         brand STRING,
#         production_year INT
#     )
# """)

# spark.sql(f"""
#     CREATE TABLE IF NOT EXISTS {SILVER_CATALOG}.{SILVER_SCHEMA}.silver_sewik_participants (
#         accident_id STRING,
#         participant_id STRING,
#         gender STRING,
#         driving_experience_years INT,
#         blood_alcohol_level DOUBLE
#     )
# """)