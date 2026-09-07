from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp


@dp.table(name = "bronze_streaming_road_accidents")
def bronze_streaming_road_accidents():
    EH_CONNECTION_STRING = dbutils.secrets.get(scope = "default2", key = "artem-evh02-connector")


    BOOTSTRAP = "evhpl24databricks02.servicebus.windows.net:9093"
    JAAS = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EH_CONNECTION_STRING}";'

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("failOnDataLoss", "false")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", "artemzharkov10_car_accidents_stream") 
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", JAAS)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "5000")
        .load()
    )
    return (
        raw_df.selectExpr("CAST(value AS STRING) as json_payload", "timestamp as eventhub_enqueued_time")
        .withColumn("ingest_timestamp", current_timestamp())
    )