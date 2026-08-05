from pyspark.sql.functions import explode, col
from pyspark import pipelines as dp

BRONZE_CATALOG = spark.conf.get("bronze_catalog", "dbr_dev")
BRONZE_SCHEMA = spark.conf.get("bronze_schema", "artemzharkov10_bronze")

BRONZE_TABLE_PATH = f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.bronze_sewik"

@dp.table(name="silver_sewik_accidents")
@dp.expect_or_fail("valid_id", "ID IS NOT NULL") 
@dp.expect_or_drop("valid_date", "DATA_ZDARZENIA IS NOT NULL")
def create_silver_accidents():
    return (
        spark.readStream.table(BRONZE_TABLE_PATH)
        .select(
            col("ID"),
            col("JEDNOSTKA_MIEJSCA"),
            col("JEDNOSTKA_LIKWIDUJACA"),
            col("JEDNOSTKA_OPERATORA"),
            col("GPS_X_GUS").cast("double").alias("GPS_X_GUS"),
            col("GPS_Y_GUS").cast("double").alias("GPS_Y_GUS"),
            col("WOJ"),
            col("GMINA"),
            col("POWIAT"),
            col("MIEJSCOWOSC"),
            col("DATA_ZDARZENIA").cast("timestamp").alias("DATA_ZDARZENIA"),
            col("DATA_ZDARZ").cast("date").alias("DATA_ZDARZ"),
            col("GODZINA_ZDARZ"),
            col("WSP_GPS_X").cast("double").alias("WSP_GPS_X"),
            col("WSP_GPS_Y").cast("double").alias("WSP_GPS_Y"),
            col("PREDKOSC_DOPUSZCZALNA").cast("int").alias("PREDKOSC_DOPUSZCZALNA"),
            col("DROGA_PUBLICZNA"),
            col("DATA_ZGLOSZENIA").cast("timestamp").alias("DATA_ZGLOSZENIA"),
            col("DATA_PRZYJAZDU").cast("timestamp").alias("DATA_PRZYJAZDU"),
            col("GEOD_KODY"),
            col("ZABU_KOD"),
            col("CHMZ_KOD"),
            col("SSWA_KOD")
        )
        .dropDuplicates(["ID"])
    )

@dp.table(name="silver_sewik_vehicles")
@dp.expect_or_drop("valid_vehicle_id", "POJAZD_ID IS NOT NULL")
def create_silver_vehicles():
    return (
        spark.readStream.table(BRONZE_TABLE_PATH)
        .select(
            col("ID").alias("ZDARZENIE_ID"),
            explode(col("POJAZDY")).alias("pojazd")
        )
        .select(
            col("ZDARZENIE_ID"),
            col("pojazd.ID").alias("POJAZD_ID"),
            col("pojazd.RODZAJ_POJAZDU").alias("RODZAJ_POJAZDU"),
            col("pojazd.MARKA").alias("MARKA"),
            col("pojazd.ROK_PRODUKCJI").cast("int").alias("ROK_PRODUKCJI")
        )
    )

@dp.table(name="silver_sewik_participants")
@dp.expect_or_drop("valid_participant_id", "UCZESTNIK_ID IS NOT NULL")
def create_silver_participants():
    return (
        spark.readStream.table(BRONZE_TABLE_PATH)
        .select(
            col("ID").alias("ZDARZENIE_ID"),
            explode(col("UCZESTNICY")).alias("uczestnik")
        )
        .select(
            col("ZDARZENIE_ID"),
            col("uczestnik.ID").alias("UCZESTNIK_ID"),
            col("uczestnik.PLEC").alias("PLEC"),
            col("uczestnik.LICZBA_LAT_KIEROWANIA").cast("int").alias("LICZBA_LAT_KIEROWANIA"),
            col("uczestnik.POZIOM_ALKOHOLU").cast("double").alias("POZIOM_ALKOHOLU")
        )
    )