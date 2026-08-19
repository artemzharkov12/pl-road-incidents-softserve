from pyspark.sql.functions import explode, col, when
from pyspark import pipelines as dp


BRONZE_CATALOG = spark.conf.get("bronze_catalog", "dbr_dev")
BRONZE_SCHEMA = spark.conf.get("bronze_schema", "artemzharkov10_bronze")

BRONZE_TABLE_PATH = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.bronze_sewik"


@dp.table(
    catalog= f"{BRONZE_CATALOG}",
    schema= f"{BRONZE_SCHEMA}",
    name="silver_sewik_accidents")
@dp.expect_or_drop("valid_date", "accident_timestamp IS NOT NULL")
@dp.expect_or_drop("valid_voivodeship", "voivodeship IS NOT NULL")
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
            when(col("WOJ") == "WOJ. MAZOWIECKIE", "Mazowieckie")
                .when(col("WOJ") == "WOJ. PODLASKIE", "Podlaskie")
                .when(col("WOJ") == "WOJ. OPOLSKIE", "Opolskie")
                .when(col("WOJ") == "WOJ. ŚLĄSKIE", "Śląskie")
                .when(col("WOJ") == "WOJ. POMORSKIE", "Pomorskie")
                .when(col("WOJ") == "WOJ. ŁÓDZKIE", "Łódzkie")
                .when(col("WOJ") == "WOJ. PODKARPACKIE", "Podkarpackie")
                .when(col("WOJ") == "WOJ. KUJAWSKO-POMORSKIE", "Kujawsko-Pomorskie")
                .when(col("WOJ") == "WOJ. WARMIŃSKO-MAZURSKIE", "Warmińsko-Mazurskie")
                .when(col("WOJ") == "WOJ. ŚWIĘTOKRZYSKIE", "Świętokrzyskie")
                .when(col("WOJ") == "WOJ. ZACHODNIOPOMORSKIE", "Zachodniopomorskie")
                .when(col("WOJ") == "WOJ. MAŁOPOLSKIE", "Małopolskie")
                .when(col("WOJ") == "WOJ. LUBUSKIE", "Lubuskie")
                .when(col("WOJ") == "WOJ. WIELKOPOLSKIE", "Wielkopolskie")
                .when(col("WOJ") == "WOJ. LUBELSKIE", "Lubelskie")
                .when(col("WOJ") == "WOJ. DOLNOŚLĄSKIE", "Dolnośląskie")
                .otherwise(col("WOJ")).alias("voivodeship"),
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

@dp.table(
    catalog= f"{BRONZE_CATALOG}",
    schema= f"{BRONZE_SCHEMA}",
    name="silver_sewik_vehicles")
@dp.expect_or_drop("valid_vehicle_id", "vehicle_id IS NOT NULL")
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

@dp.table(
    catalog= f"{BRONZE_CATALOG}",
    schema= f"{BRONZE_SCHEMA}",
    name="silver_sewik_participants")
@dp.expect_or_drop("valid_participant_id", "participant_id IS NOT NULL")
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