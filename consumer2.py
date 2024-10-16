from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, from_unixtime, window, col
from pyspark.sql.functions import count as spark_count
from pyspark.sql import Row
import time

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("HeartRateAnalytics") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .getOrCreate()

# Define Kafka parameters
kafka_bootstrap_servers = "localhost:9092"
topic2 = "topic2"

# Global variable to maintain overall count
overall_count_above_90 = 0

# Define the input DataFrame representing the Kafka stream for topic2
df_topic2 = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic2) \
    .load()

# Convert the binary value column 'value' to string and cast to integer
df_topic2 = df_topic2.selectExpr("CAST(value AS STRING) AS value") \
    .selectExpr("CAST(value AS INT) AS heart_rate")

# Add a timestamp column with the current time
df_topic2 = df_topic2.withColumn("timestamp", current_timestamp())

# Count values above 70 in topic2 within a window of 10 seconds
windowed_df_topic2 = df_topic2 \
    .filter(df_topic2.heart_rate > 90) \
    .groupBy(window("timestamp", "10 seconds")) \
    .agg(spark_count("*").alias("count_above_90"))

# Process each batch of data for topic2
def process_batch_topic2(batch_df, batch_id):
    global overall_count_above_90
    start_time = time.time()
    count_row = batch_df.select("count_above_90").first()
    count_above_90 = count_row[0] if count_row is not None else None
    if count_above_90 is not None:
        overall_count_above_90 += count_above_90
        print(f"Count of values above 90 for the last 10 seconds: {count_above_90}")
        print(f"Overall count of values above 90: {overall_count_above_90}")
    end_time = time.time()  # Record the end time
    processing_time = end_time - start_time
    print(f"Processing time for batch {batch_id}: {processing_time} seconds")


# Apply the processing function to each batch in the streaming DataFrame for topic2
query_topic2 = windowed_df_topic2 \
    .writeStream \
    .foreachBatch(process_batch_topic2) \
    .outputMode("complete") \
    .start()

# Wait for the termination of the query for topic2
query_topic2.awaitTermination()
