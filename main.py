from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("salah") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Create a simple DataFrame to test Spark
data = [("Salah", 21), ("Ali", 25), ("Mona", 23)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Show the DataFrame
df.show()

# Stop the Spark session
spark.stop()