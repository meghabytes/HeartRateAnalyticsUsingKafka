from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, max as spark_max, window
import time 
# Initialize SparkSession
spark = SparkSession.builder \
    .appName("HeartRateAnalytics") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .getOrCreate()

# Define Kafka parameters
kafka_bootstrap_servers = "localhost:9092"
topic = "topic1"

# Define the input DataFrame representing the Kafka stream
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic) \
    .load()

# Convert the binary value column 'value' to string and cast to integer
df = df.selectExpr("CAST(value AS STRING) AS value") \
    .selectExpr("CAST(value AS INT) AS heart_rate")

# Add a timestamp column with the current time
df = df.withColumn("timestamp", current_timestamp())

# Define a window of 10 seconds and calculate the maximum heart rate
windowed_df = df \
    .groupBy(window("timestamp", "10 seconds", "5 seconds")) \
    .agg(spark_max("heart_rate").alias("max_heart_rate"))

# Define the maximum heart rate variable
MAXI = float("-inf")

# Process each batch of data
# Process each batch of data
def process_batch(batch_df, batch_id):
    global MAXI
    start_time = time.time()
    max_heart_rate_row = batch_df.select("max_heart_rate").first()
    max_heart_rate = max_heart_rate_row[0] if max_heart_rate_row is not None else None
    if max_heart_rate is not None and max_heart_rate > MAXI:
        MAXI = max_heart_rate
    print(f"Maximum heart rate for the last 10 seconds: {max_heart_rate}, Overall maximum heart rate: {MAXI}")
    end_time = time.time()  # Record the end time
    processing_time = end_time - start_time
    print(f"Processing time for batch {batch_id}: {processing_time} seconds")


# Apply the processing function to each batch in the streaming DataFrame
query = windowed_df \
    .writeStream \
    .foreachBatch(process_batch) \
    .outputMode("complete") \
    .start()

# Wait for the termination of the query
query.awaitTermination()
