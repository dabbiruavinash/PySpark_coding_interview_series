
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("EcommerceWatermarking").getOrCreate()

# Define schema for streaming e-commerce orders
order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_amount", DoubleType(), True),
    StructField("event_time", StringType(), True)  # event time from source
])

# Read streaming data (simulated from Kafka or files)
streaming_df = spark.readStream \
    .format("json") \
    .schema(order_schema) \
    .option("maxFilesPerTrigger", 1) \
    .load("/path/to/streaming/input/dir")

# Convert event_time to timestamp and apply watermark (allow 10 min late data)
streaming_with_watermark = streaming_df \
    .withColumn("event_timestamp", to_timestamp(col("event_time"))) \
    .withWatermark("event_timestamp", "10 minutes")

# Aggregate or process (e.g., total amount per customer in a tumbling window)
windowed_agg = streaming_with_watermark \
    .groupBy(
        col("customer_id"),
        window(col("event_timestamp"), "5 minutes")
    ) \
    .sum("order_amount") \
    .withColumn("processed_time", current_timestamp())

# Write to Delta table
query = windowed_agg.writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("checkpointLocation", "/tmp/checkpoints/ecommerce_watermark") \
    .table("ecommerce_windowed_agg")

query.awaitTermination()