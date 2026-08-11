from pyspark.sql.functions import explode, col
import dlt

BRONZE_CATALOG = spark.conf.get("bronze_catalog", "dbr_dev")
BRONZE_SCHEMA = spark.conf.get("bronze_schema", "artemzharkov10_bronze")

BRONZE_TABLE_PATH = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.bronze_sewik"

@dlt.table(name="silver_sewik_accidents")
@dlt.expect_or_fail("valid_id", "accident_id IS NOT NULL") 
@dlt.expect_or_drop("valid_date", "accident_timestamp IS NOT NULL")
def create_silver_accidents():
    return (
        spark.readStream.table(BRONZE_TABLE_PATH)
        .select(
            col("ID").alias("accident_id"),
            col("JEDNOSTKA_MIEJSCA").alias("police_unit_local"),
            col("JEDNOSTKA_LIKWIDUJACA").alias("police_unit_handling"),
            col("JEDNOSTKA_OPERATORA").alias("police_unit_operator"),
            col("GPS_X_GUS").cast("double").alias("gps_x_gus"),
            col("GPS_Y_GUS").cast("double").alias("gps_y_gus"),
            col("WOJ").alias("voivodeship"),
            col("GMINA").alias("municipality"),
            col("POWIAT").alias("district"),
            col("MIEJSCOWOSC").alias("city"),
            col("DATA_ZDARZENIA").cast("timestamp").alias("accident_timestamp"),
            col("DATA_ZDARZ").cast("date").alias("accident_date"),
            col("GODZINA_ZDARZ").alias("accident_time"),
            col("WSP_GPS_X").cast("double").alias("gps_x"),
            col("WSP_GPS_Y").cast("double").alias("gps_y"),
            col("PREDKOSC_DOPUSZCZALNA").cast("int").alias("speed_limit"),
            col("DROGA_PUBLICZNA").alias("public_road"),
            col("DATA_ZGLOSZENIA").cast("timestamp").alias("report_timestamp"),
            col("DATA_PRZYJAZDU").cast("timestamp").alias("arrival_timestamp"),
            col("GEOD_KODY").alias("geod_code"),
            col("ZABU_KOD").alias("built_up_area_code"),
            col("CHMZ_KOD").alias("light_conditions_code"),
            col("SSWA_KOD").alias("traffic_lights_code")
        )
        .dropDuplicates(["accident_id"])
    )

@dlt.table(name="silver_sewik_vehicles")
@dlt.expect_or_drop("valid_vehicle_id", "vehicle_id IS NOT NULL")
def create_silver_vehicles():
    return (
        spark.readStream.table(BRONZE_TABLE_PATH)
        .select(
            col("ID").alias("accident_id"),
            explode(col("POJAZDY")).alias("pojazd")
        )
        .select(
            col("accident_id"),
            col("pojazd.ID").alias("vehicle_id"),
            col("pojazd.RODZAJ_POJAZDU").alias("vehicle_type"),
            col("pojazd.MARKA").alias("brand"),
            col("pojazd.ROK_PRODUKCJI").cast("int").alias("production_year")
        )
    )

@dlt.table(name="silver_sewik_participants")
@dlt.expect_or_drop("valid_participant_id", "participant_id IS NOT NULL")
def create_silver_participants():
    return (
        spark.readStream.table(BRONZE_TABLE_PATH)
        .select(
            col("ID").alias("accident_id"),
            explode(col("UCZESTNICY")).alias("uczestnik")
        )
        .select(
            col("accident_id"),
            col("uczestnik.ID").alias("participant_id"),
            col("uczestnik.PLEC").alias("gender"),
            col("uczestnik.LICZBA_LAT_KIEROWANIA").cast("int").alias("driving_experience_years"),
            col("uczestnik.POZIOM_ALKOHOLU").cast("double").alias("blood_alcohol_level")
        )
    )