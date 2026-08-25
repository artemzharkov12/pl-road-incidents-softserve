from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

EH_CONN_STR = dbutils.secrets.get(scope="default2", key="artem-evh-connector")
BOOTSTRAP = "evhpl24databricks.servicebus.windows.net:9093"
JAAS = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EH_CONN_STR}";'

@dp.table(
    name="bronze_streaming_weather"
)
def bronze_streaming_weather_data():
    raw_df = (
        spark.readStream
        .format("kafka")
        # .option("failOnDataLoss", "false") # just for dev org
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", "artemzharkov10_evh") 
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", JAAS)
        .option("startingOffsets", "earliest")
        .load()
    )
    
    return (
        raw_df.selectExpr("CAST(value AS STRING) as json_payload", "timestamp as eventhub_enqueued_time")
        .withColumn("ingest_timestamp", current_timestamp())
    )