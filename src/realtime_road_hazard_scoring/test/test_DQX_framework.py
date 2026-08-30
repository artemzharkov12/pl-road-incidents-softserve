import sys
import subprocess
import importlib

subprocess.check_call([
    sys.executable, "-m", "pip", "install", "databricks-labs-dqx==0.16.0"
])
importlib.invalidate_caches()

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRowRule

from datetime import datetime, timedelta
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
from pyspark.dbutils import DBUtils
dbutils = DBUtils(spark)

dbutils.widgets.text("silver_catalog", "dbr_dev")
dbutils.widgets.text("silver_schema", "artemzharkov10_silver")
SILVER_CATALOG = dbutils.widgets.get("silver_catalog")
SILVER_SCHEMA = dbutils.widgets.get("silver_schema")

SILVER_TABLE = f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_streaming_weather"
QUARANTINE_TABLE = f"{SILVER_CATALOG}.{SILVER_SCHEMA}.silver_streaming_weather_quarantine"

dq_engine = DQEngine()
df_silver = spark.read.table(SILVER_TABLE)

time_threshold = datetime.now() - timedelta(days=1)
df_recent = df_silver.filter(F.col("weather_time") >= time_threshold)

if not df_recent.rdd.isEmpty():
    dq_checks = [
        DQRowRule(
            name="valid_latitude_poland",
            criticality="error",
            check_func=check_funcs.is_in_range,
            column="latitude",
            check_func_kwargs={"min_limit": 49.0, "max_limit": 54.8}
        ),
        DQRowRule(
            name="valid_precipitation",
            criticality="error",
            check_func=check_funcs.not_less_than,
            column="precipitation_mm",
            check_func_kwargs={"limit": 0}
        )
    ]
    valid_df, error_df = dq_engine.apply_checks_and_split(df_recent, dq_checks)
    error_df.write.format("delta").mode("append").saveAsTable(QUARANTINE_TABLE)