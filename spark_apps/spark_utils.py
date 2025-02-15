from pyspark.sql.functions import from_unixtime, to_date, date_add, lit,col
from pyspark.sql.types import *



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
