# Databricks notebook source
dbutils.widgets.text("bronze_catalog","dbr_dev")
dbutils.widgets.text("bronze_schema","artemzharkov10_silver")

BRONZE_CATALOG = dbutils.widgets.get("bronze_catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_CATALOG}.{BRONZE_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {BRONZE_CATALOG}.{BRONZE_SCHEMA}.checkpoints")

# COMMAND ----------

# MAGIC %md
# MAGIC Since a declarative approach was used in the lab, this code simply serves as a reminder of how the classic implementation method works.

# COMMAND ----------

# spark.sql(f"""
#     CREATE TABLE IF NOT EXISTS {BRONZE_CATALOG}.{BRONZE_SCHEMA}.sewik_silver (
#         ID STRING,
#         JEDNOSTKA_MIEJSCA STRING,
#         JEDNOSTKA_LIKWIDUJACA STRING,
#         JEDNOSTKA_OPERATORA STRING,
#         GPS_X_GUS STRING,
#         GPS_Y_GUS STRING,
#         WOJ STRING,
#         GMINA STRING,
#         POWIAT STRING,
#         MIEJSCOWOSC STRING,
#         DATA_ZDARZENIA STRING,
#         DATA_ZDARZ STRING,
#         GODZINA_ZDARZ STRING,
#         WSP_GPS_X STRING,
#         WSP_GPS_Y STRING,
#         PREDKOSC_DOPUSZCZALNA STRING,
#         DROGA_PUBLICZNA STRING,
#         DATA_ZGLOSZENIA STRING,
#         DATA_PRZYJAZDU STRING,
#         GEOD_KODY ARRAY<STRING>,
#         ZABU_KOD STRING,
#         CHMZ_KOD STRING,
#         SSWA_KOD STRING,
        
#         POJAZDY ARRAY<STRUCT<
#             ID: STRING,
#             RODZAJ_POJAZDU: STRING,
#             MARKA: STRING,
#             ROK_PRODUKCJI: STRING
#         >>,
#         UCZESTNICY ARRAY<STRUCT<
#             ID: STRING,
#             PLEC: STRING,
#             LICZBA_LAT_KIEROWANIA: STRING,
#             POZIOM_ALKOHOLU: STRING
#         >>,
#         _rescued_data STRING,
#         source_filename STRING,
#         ingest_timestamp TIMESTAMP
#     )
# """)