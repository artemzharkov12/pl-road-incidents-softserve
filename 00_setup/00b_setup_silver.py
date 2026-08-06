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
#         ID STRING,
#         JEDNOSTKA_MIEJSCA STRING,
#         JEDNOSTKA_LIKWIDUJACA STRING,
#         JEDNOSTKA_OPERATORA STRING,
#         GPS_X_GUS DOUBLE,
#         GPS_Y_GUS DOUBLE,
#         WOJ STRING,
#         GMINA STRING,
#         POWIAT STRING,
#         MIEJSCOWOSC STRING,
#         DATA_ZDARZENIA TIMESTAMP,
#         DATA_ZDARZ DATE,
#         GODZINA_ZDARZ STRING,
#         WSP_GPS_X DOUBLE,
#         WSP_GPS_Y DOUBLE,
#         PREDKOSC_DOPUSZCZALNA INT,
#         DROGA_PUBLICZNA STRING,
#         DATA_ZGLOSZENIA TIMESTAMP,
#         DATA_PRZYJAZDU TIMESTAMP,
#         GEOD_KODY ARRAY<STRING>,
#         ZABU_KOD STRING,
#         CHMZ_KOD STRING,
#         SSWA_KOD STRING
#     )
# """)

# spark.sql(f"""
#     CREATE TABLE IF NOT EXISTS {SILVER_CATALOG}.{SILVER_SCHEMA}.silver_sewik_vehicles (
#         ZDARZENIE_ID STRING,
#         POJAZD_ID STRING,
#         RODZAJ_POJAZDU STRING,
#         MARKA STRING,
#         ROK_PRODUKCJI INT
#     )
# """)

# spark.sql(f"""
#     CREATE TABLE IF NOT EXISTS {SILVER_CATALOG}.{SILVER_SCHEMA}.silver_sewik_participants (
#         ZDARZENIE_ID STRING,
#         UCZESTNIK_ID STRING,
#         PLEC STRING,
#         LICZBA_LAT_KIEROWANIA INT,
#         POZIOM_ALKOHOLU DOUBLE
#     )
# """)