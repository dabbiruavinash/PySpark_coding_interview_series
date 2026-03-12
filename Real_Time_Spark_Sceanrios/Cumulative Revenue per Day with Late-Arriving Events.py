from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder
             .appName("cumulativeRevenueWithLateEvents")
             .config("spark.sql.extensioins", "io.detla.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate()

schema = StructType([
    StructField("event_id", StringType()),
    StructField("order_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("revenue", DoubleType()),
    StructField("product_id", StringType()),
    StructField("customer_id", StringType())
])

streaming_df = spark.readStream.format("delta").option("maxFilePerTrigger", 1).table("source_events")

withmarked_df = streaming_df.withWatermark("event_time", "1 hour")
.select(col("event_id"),
        col("order_id"),
        col("event_time"),
        col("revenue"),
        col("product_id"),
        col("customer_id"),
        to_date(col("event_time")).alias("event_date"))

# calculate daily revenue with cumulative sum

daily_revenue = watermarked_df.groupBy(window(col("event_time"), "1 day"), col("event_date")
                             .agg(sum("revenue").alias("daily_revenue"), count("event_id").alias("event_count"))
                             .select(
                              col("event_date"),
                              col("daily_revenue"),
                              col("event_count"))


# Add cumulative revenue using window function (for batch processing)
# For streaming, we'll use Delta Lake merge for upserts

def write_to_dela_with_merge(microbatch_df, batch_id):
       batch_aggregated = microbatch_df.groupby(to_date("event_time").alias("event_time")).agg(
                                             sum("revenue").alias("new_revenue"), count("event_id").alias("new_event_count"))

       # merge into target delta table
       from delta.tables import DeltaTable

       if DeltaTable.isDeltaTable(spark, "/path/to/daily_revenue"):
          delta_table = DeltaTable.forPath(spark, "/path/to/daily_revenue")

        delta_table.alias("target") \
            .merge(
                batch_aggregated.alias("source"),
                "target.event_date = source.event_date"
            ) \
            .whenMatchedUpdate(set={
                "daily_revenue": col("target.daily_revenue") + col("source.new_revenue"),
                "event_count": col("target.event_count") + col("source.new_event_count"),
                "last_updated": lit(current_timestamp())
            }) \
            .whenNotMatchedInsert(values={
                "event_date": col("source.event_date"),
                "daily_revenue": col("source.new_revenue"),
                "event_count": col("source.new_event_count"),
                "last_updated": lit(current_timestamp())
            }) \
            .execute()
    else:
        # First batch - write directly
        batch_aggregated \
            .withColumn("last_updated", current_timestamp()) \
            .write \
            .format("delta") \
            .mode("append") \
            .save("/path/to/daily_revenue")

# Write stream with foreachBatch for merge logic
streaming_query = watermarked_df \
    .writeStream \
    .foreachBatch(write_to_delta_with_merge) \
    .outputMode("update") \
    .option("checkpointLocation", "/path/to/checkpoint") \
    .trigger(processingTime="1 minute") \
    .start()

