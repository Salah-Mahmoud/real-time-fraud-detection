from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, lit, from_unixtime, date_add, to_date, monotonically_increasing_id
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType, DateType
from pyspark.ml import PipelineModel
from utility import *

transaction_schema = StructType([
    StructField("trans_date_trans_time", LongType(), True),
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
    StructField("dob", LongType(), True),
    StructField("trans_num", StringType(), True),
    StructField("unix_time", LongType(), True),
    StructField("merch_lat", DoubleType(), True),
    StructField("merch_long", DoubleType(), True)
])

KAFKA_CONFIG = {
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

spark = SparkSession.builder \
    .appName("KafkaTransactionConsumer") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
            "org.apache.kafka:kafka-clients:3.5.1,"
            "org.postgresql:postgresql:42.7.1") \
    .getOrCreate()

raw_stream_df = spark \
    .readStream \
    .format("kafka") \
    .options(**KAFKA_CONFIG) \
    .load() \
    .selectExpr("CAST(value AS STRING) as value")

transactions_df = raw_stream_df.select(
    from_json(
        col("value"),
        StructType([
            StructField("payload", StructType([
                StructField("after", transaction_schema, True)
            ]), True)
        ])
    ).alias("data")
).select("data.payload.after.*")

MODEL_PATH = "/opt/bitnami/spark/spark-data/random_forest_model"
fraud_detection_model = PipelineModel.load(MODEL_PATH)


def write_to_postgres(batch_df, batch_id):
    batch_df = batch_df.withColumn(
        "trans_date_trans_time",
        from_unixtime((col("trans_date_trans_time") / 1000000).cast("integer")).cast("timestamp")
    )

    batch_df = batch_df.withColumn(
        "dob",
        to_date(date_add(lit("1970-01-01").cast(DateType()), col("dob").cast("int")))
    )

    feature_df = batch_df.select(
        "amt", "lat", "long", "city_pop", "unix_time", "merch_lat", "merch_long"
    )

    predictions_df = fraud_detection_model.transform(feature_df)

    result_df = batch_df.withColumn("id", monotonically_increasing_id()).join(
        predictions_df.withColumn("id", monotonically_increasing_id()).select("id",
                                                                              col("prediction").alias("is_fraud")),
        on="id",
        how="inner"
    ).drop("id")

    fraud_transactions = result_df.select("trans_num", "is_fraud").collect()

    for row in fraud_transactions:
        short_trans_num = "#" + row["trans_num"].split("-")[0]
        if row["is_fraud"] == 1:
            send_message(f"🔻 Please cancel this transaction {short_trans_num} because it is fraud.")
        else:
            send_message(f"🟢 Success: Transaction {short_trans_num} is valid.")

    result_df.write \
        .format("jdbc") \
        .option("url", DB_CONFIG["url"]) \
        .option("dbtable", "processed_transactions") \
        .option("user", DB_CONFIG["properties"]["user"]) \
        .option("password", DB_CONFIG["properties"]["password"]) \
        .option("driver", DB_CONFIG["properties"]["driver"]) \
        .mode("append") \
        .save()


query = transactions_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()
