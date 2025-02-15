from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
from pyspark.ml import PipelineModel

after_schema = StructType([
    StructField("trans_date_trans_time", TimestampType(), True),
    StructField("cc_num", LongType(), True),
    StructField("merchant", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amt", DoubleType(), True),
    StructField("first", StringType(), True),
    StructField("last", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("long", DoubleType(), True),
    StructField("city_pop", IntegerType(), True),
    StructField("job", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("trans_num", StringType(), True),
    StructField("unix_time", LongType(), True),
    StructField("merch_lat", DoubleType(), True),
    StructField("merch_long", DoubleType(), True)
])

kafka_input_config = {
    "kafka.bootstrap.servers": "broker:29092",
    "subscribe": "cdc.public.transaction",
    "startingOffsets": "latest",
    "failOnDataLoss": "false"
}

DB_CONFIG = {
    "url": "jdbc:postgresql://postgres:5432/fraud_detection",
    "properties": {
        "user": "admin",
        "password": "admin",
        "driver": "org.postgresql.Driver"
    }
}

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaTransactionConsumer") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
            "org.apache.kafka:kafka-clients:3.5.1,"
            "org.postgresql:postgresql:42.7.1"
            ) \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .options(**kafka_input_config) \
    .load() \
    .selectExpr("CAST(value AS STRING) as value")

parsed_df = df.select(
    from_json(
        col("value"),
        StructType([
            StructField("payload", StructType([
                StructField("after", after_schema, True)
            ]), True)
        ])
    ).alias("data")
).select("data.payload.after.*")

model_path = "/opt/bitnami/spark/spark-data/random_forest_model"
pipeline_model_loaded = PipelineModel.load(model_path)



def write_to_postgres(batch_df, batch_id):
    batch_df = batch_df.withColumn(
        "trans_date_trans_time",
        from_unixtime(col("trans_date_trans_time") / 1000000).cast("timestamp")
    )

    batch_df = batch_df.withColumn(
        "dob",
        to_date(date_add(lit("1970-01-01").cast(DateType()), col("dob").cast("int")))
    )
    prediction_df = batch_df.select(
        "amt", "lat", "long", "city_pop", "unix_time", "merch_lat", "merch_long"
    )

    predictions = pipeline_model_loaded.transform(prediction_df)

    predictions = predictions.withColumnRenamed("prediction", "is_fraud")

    result_df = batch_df.join(
        predictions.select("is_fraud"),
        how="inner"
    )

    result_df.write \
        .format("jdbc") \
        .option("url", DB_CONFIG["url"]) \
        .option("dbtable", "processed_transactions") \
        .option("user", DB_CONFIG["properties"]["user"]) \
        .option("password", DB_CONFIG["properties"]["password"]) \
        .option("driver", DB_CONFIG["properties"]["driver"]) \
        .mode("append") \
        .save()

query = parsed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()