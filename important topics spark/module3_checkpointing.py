
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

spark = SparkSession.builder.appName("EcommerceCheckpointing").getOrCreate()

# Base checkpoint directory (DBFS or cloud storage)
checkpoint_base = "/tmp/checkpoints/ecommerce"

# ----- Batch ETL with checkpointing (using DataFrame checkpoint) -----
df = spark.read.format("delta").table("ecommerce_raw_orders")

# Set checkpoint directory for the session
spark.sparkContext.setCheckpointDir(f"{checkpoint_base}/batch")

# Transformations
transformed_df = df.filter(col("order_amount") > 100).withColumn("etl_timestamp", current_timestamp())

# Checkpoint to truncate lineage
checkpointed_df = transformed_df.checkpoint(eager=True)

# Write results
checkpointed_df.write.format("delta").mode("overwrite").save("/tmp/ecommerce_high_value_orders")

# ----- Streaming with checkpointing -----
streaming_df = spark.readStream \
    .format("delta") \
    .option("maxFilesPerTrigger", 1) \
    .load("/tmp/streaming_source")

query = streaming_df.writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("checkpointLocation", f"{checkpoint_base}/streaming_checkpoint") \
    .toTable("ecommerce_streaming_sink")

# Recovery after failure - Databricks automatically resumes from checkpoint