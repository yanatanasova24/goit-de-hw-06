from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    avg,
    to_json,
    struct,
    lit,
)


from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


BOOTSTRAP_SERVERS = "localhost:9092"

SENSORS_TOPIC = "building_sensors_yana"
TEMPERATURE_TOPIC = "temperature_alerts_yana"
HUMIDITY_TOPIC = "humidity_alerts_yana"

ALERTS_CSV_PATH = "alerts_conditions.csv"

spark = (
    SparkSession.builder
    .appName("BuildingSensorsAlerts")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


sensor_schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
])


kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", SENSORS_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

sensor_data = (
    kafka_stream
    .selectExpr("CAST(value AS STRING) AS json_data")
    .select(
        from_json(col("json_data"), sensor_schema).alias("data")
    )
    .select("data.*")
    .withColumn("timestamp", to_timestamp(col("timestamp")))
    .filter(
        col("sensor_id").isNotNull()
        & col("timestamp").isNotNull()
        & col("temperature").isNotNull()
        & col("humidity").isNotNull()
    )
)

aggregated_data = (
    sensor_data
    .withWatermark("timestamp", "10 seconds")
    .groupBy(
        col("sensor_id"),
        window(col("timestamp"), "1 minute", "30 seconds")
    )
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity"),
    )
    .select(
        col("sensor_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_temperature"),
        col("avg_humidity"),
    )
)


alerts = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("sep", ",")
    .csv(ALERTS_CSV_PATH)
)


def process_batch(batch_df, batch_id):
    if not batch_df.take(1):
        return

    candidates = batch_df.crossJoin(alerts)

    temperature_ok = (
        (
            (col("temperature_min") == -999)
            & (col("temperature_max") == -999)
        )
        |
        (
            (col("avg_temperature") >= col("temperature_min"))
            & (col("avg_temperature") <= col("temperature_max"))
        )
    )

    humidity_ok = (
        (
            (col("humidity_min") == -999)
            & (col("humidity_max") == -999)
        )
        |
        (
            (col("avg_humidity") >= col("humidity_min"))
            & (col("avg_humidity") <= col("humidity_max"))
        )
    )

    matched = (
        candidates
        .filter(temperature_ok & humidity_ok)
        .select(
            col("window_start"),
            col("window_end"),
            col("avg_temperature"),
            col("avg_humidity"),
            col("code"),
            col("sensor_id"),
            col("message"),
        )
    )

    alert_json = to_json(
        struct(
            col("sensor_id").alias("sensor_id"),
            col("window_end").alias("timestamp"),
            col("avg_temperature").alias("temperature"),
            col("avg_humidity").alias("humidity"),
            col("code").alias("code"),
            col("message").alias("message"),
            col("window_start").alias("window_start"),
            col("window_end").alias("window_end"),
        )
    )

    humidity_output = (
        matched
        .filter(col("code").isin(101, 102))
        .select(
            col("code").cast("string").alias("key"),
            alert_json.alias("value"),
        )
    )

    temperature_output = (
        matched
        .filter(col("code").isin(103, 104))
        .select(
            col("code").cast("string").alias("key"),
            alert_json.alias("value"),
        )
    )

    (
        humidity_output.write
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("topic", HUMIDITY_TOPIC)
        .save()
    )

    (
        temperature_output.write
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("topic", TEMPERATURE_TOPIC)
        .save()
    )

    print(f"Batch {batch_id} оброблено.")


query = (
    aggregated_data.writeStream
    .outputMode("append")
    .foreachBatch(process_batch)
    .option(
        "checkpointLocation",
        "./checkpoints/building_sensors_alerts",
    )
    .start()
)

print("Spark Streaming запущено. Очікування даних...")

query.awaitTermination()