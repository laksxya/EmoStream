
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    window, col, from_json, to_json, count, lit, struct, collect_list
)
from pyspark.sql.types import StructType, StringType, TimestampType
import shutil
import os

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "emoji-topic"
OUTPUT_TOPIC = "aggregated-emoji-topic"
CHECKPOINT_LOCATION = "/tmp/emoji-checkpoint"

# Clean up checkpoint directory if it exists
if os.path.exists(CHECKPOINT_LOCATION):
    shutil.rmtree(CHECKPOINT_LOCATION)

# Define the schema of incoming emoji data
schema = StructType() \
    .add("user_id", StringType()) \
    .add("emoji_type", StringType()) \
    .add("timestamp", TimestampType())

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("EmojiAggregator") \
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
    .getOrCreate()

# Set Spark log level to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Read data from Kafka
emoji_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", INPUT_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Deserialize the JSON payload
emoji_data = emoji_stream.select(from_json(col("value").cast(
    "string"), schema).alias("data")).select("data.*")

# Add watermark and perform single window-based aggregation
windowed_counts = emoji_data \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(
        window(col("timestamp"), "2 seconds"),
        col("emoji_type")
    ) \
    .agg(
        count("*").alias("count")
    ) \
    .withColumn("scaled_count", (col("count") / lit(5)).cast("int")) \
    .select(
        col("window").alias("window"),
        struct(
            col("emoji_type"),
            col("scaled_count")
        ).alias("emoji_info")
    ) \
    .groupBy("window") \
    .agg(
        collect_list("emoji_info").alias("emoji_data")
    )

# Prepare final output
final_output = windowed_counts.select(
    col("window.start").alias("timestamp"),
    col("emoji_data")
).select(
    to_json(struct("timestamp", "emoji_data")).alias("value")
)

# Write the processed data back to Kafka
query = final_output.writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", OUTPUT_TOPIC) \
    .start()

try:
    query.awaitTermination()
except Exception as e:
    print(f"Streaming query terminated with error: {str(e)}")
    # Clean up checkpoint directory on error
    if os.path.exists(CHECKPOINT_LOCATION):
        shutil.rmtree(CHECKPOINT_LOCATION)
finally:
    # Ensure proper cleanup
    if query is not None:
        query.stop()
    spark.stop()
