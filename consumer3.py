from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, max as spark_max, min as spark_min, window
import time
# Initialize SparkSession
spark = SparkSession.builder \
    .appName("HeartRateAnalytics") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .getOrCreate()

# Define Kafka parameters
kafka_bootstrap_servers = "localhost:9092"

topic3 = "topic3"


# Define the input DataFrame representing the Kafka stream for topic3
df_topic3 = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic3) \
    .load()

# Convert the binary value column 'value' to string and cast to integer for topic3
df_topic3 = df_topic3.selectExpr("CAST(value AS STRING) AS value") \
    .selectExpr("CAST(value AS INT) AS heart_rate")

# Add a timestamp column with the current time for topic3
df_topic3 = df_topic3.withColumn("timestamp", current_timestamp())

# Define a window of 10 seconds and calculate the minimum heart rate for topic3
windowed_df_topic3 = df_topic3 \
    .groupBy(window("timestamp", "10 seconds")) \
    .agg(spark_min("heart_rate").alias("min_heart_rate"))

# Define the minimum heart rate variable for topic3
MINI_topic3 = float("inf")

# Process each batch of data for topic3
def process_batch_topic3(batch_df, batch_id):
    global MINI_topic3
    start_time = time.time()
    min_heart_rate_row = batch_df.select("min_heart_rate").first()
    min_heart_rate = min_heart_rate_row[0] if min_heart_rate_row is not None else None
    if min_heart_rate is not None and min_heart_rate < MINI_topic3:
        MINI_topic3 = min_heart_rate
    print(f"Minimum heart rate for the last 10 seconds (topic3): {min_heart_rate}, Overall minimum heart rate (topic3): {MINI_topic3}")
    end_time = time.time()  # Record the end time
    processing_time = end_time - start_time
    print(f"Processing time for batch {batch_id}: {processing_time} seconds")

# Apply the processing function to each batch in the streaming DataFrame for topic3
query_topic3 = windowed_df_topic3 \
    .writeStream \
    .foreachBatch(process_batch_topic3) \
    .outputMode("complete") \
    .start()

query_topic3.awaitTermination()

