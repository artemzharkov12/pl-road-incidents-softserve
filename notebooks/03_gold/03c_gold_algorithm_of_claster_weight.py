# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F
from itertools import chain

# COMMAND ----------

dbutils.widgets.text("gold_catalog", "dbr_dev")
dbutils.widgets.text("gold_schema", "artemzharkov10_gold")

GOLD_CATALOG = dbutils.widgets.get("gold_catalog")
GOLD_SCHEMA = dbutils.widgets.get("gold_schema")

# COMMAND ----------

#Contain history weather of 2020-2024 of each voivodeship
df_weather_full = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_weather_clusters")
#Contain each car accident with weather cluster,voivodeship,time of accident
df_accidents = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_accident_weather_analytics")

GOLD_RISK_INDEX_TABLE = f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_weather_cluster_weights"

# COMMAND ----------

df_weather_counter = (
    df_weather_full
    .groupBy("weather_claster")
    .count()
    .withColumnRenamed("count", "claster_count")
    .withColumn("hours_count", F.col("claster_count")) # one accodent = 1 hour step
)

df_accident_counter = (
    df_accidents
    .groupBy("weather_claster")
    .count()
    .withColumnRenamed("count", "accident_count")
)

# COMMAND ----------



# Monthly traffic weighting coefficients (January – December) - all information regarding that in documentation
traffic_weights = {
    1: 0.857, 2: 0.910, 3: 0.936, 4: 0.988, 5: 1.030, 6: 1.069,
    7: 1.126, 8: 1.133, 9: 1.033, 10: 1.027, 11: 0.964, 12: 0.936
}

mapping_expr = F.create_map([F.lit(x) for x in chain(*traffic_weights.items())])

# filtering for taking just a day hours (avoid night hours) and adding a traffic weight regarding month type 
df_weather_day = (
    df_weather_full.filter(F.hour("time").between(6, 22))
    .withColumn("month", F.month("time"))
    .withColumn("traffic_weight", mapping_expr.getItem(F.col("month")))
)
df_accidents_day = df_accidents.filter(F.hour("join_time").between(6, 22))



# Calculation of traffic-adjusted hours and number of accidents by cluster
df_weather_cluster = (
    df_weather_day.groupBy("weather_claster")
    .agg(
        F.count("*").alias("hours_count"),
        F.sum("traffic_weight").alias("traffic_adjusted_hours")
    )
)
df_accidents_cluster = (
    df_accidents_day.groupBy("weather_claster")
    .agg(F.count("*").alias("accident_count"))
)



# sums up all traffic-adjusted hours across all weather clusters combined.
total_adjusted_hours = df_weather_cluster.select(F.sum("traffic_adjusted_hours")).collect()[0][0]
# Sums up all accidents from the `accident_count` column across all clusters and retrieves the total count.
total_accidents = df_accidents_cluster.select(F.sum("accident_count")).collect()[0][0]
baseline_freq = total_accidents / total_adjusted_hours



# 4. algorithm implamentation
df_final_risk = (
    df_weather_cluster.join(
        df_accidents_cluster, 
        on="weather_claster", 
        how="left"
    )
    .fillna(0, subset=["accident_count"])
    .withColumn(
        "cluster_freq", 
        F.col("accident_count") / F.col("traffic_adjusted_hours")
    )
    .withColumn(
        "normalized_risk_index", 
        F.round(F.col("cluster_freq") / F.lit(baseline_freq), 2)
    )
    .select(
        "weather_claster", 
        "hours_count", 
        "traffic_adjusted_hours",
        "accident_count", 
        "normalized_risk_index"
    )
    .orderBy(F.col("normalized_risk_index").desc())
)
# display(df_final_risk)

# COMMAND ----------

(df_final_risk.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(GOLD_RISK_INDEX_TABLE)
)

# COMMAND ----------

# display(df_final_risk)