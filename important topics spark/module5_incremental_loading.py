
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, lit, current_timestamp

spark = SparkSession.builder.appName("EcommerceIncrementalLoad").getOrCreate()

# Method 1: Using watermark column (last_updated)
def incremental_load_with_watermark(source_path, target_table, watermark_column="last_updated"):
    # Get last max watermark from target
    try:
        last_watermark = spark.sql(f"SELECT MAX({watermark_column}) as max_ts FROM {target_table}").collect()[0]["max_ts"]
        if last_watermark is None:
            last_watermark = "1900-01-01"
    except:
        last_watermark = "1900-01-01"
        print(f"Target table {target_table} not found, doing full load")
    
    # Read incremental data
    incremental_df = spark.read.format("delta").load(source_path) \
        .filter(col(watermark_column) > last_watermark)
    
    # Write incrementally
    incremental_df.write.format("delta").mode("append").saveAsTable(target_table)
    
    print(f"Incremental load completed. Processed {incremental_df.count()} records")

# Method 2: Using Auto Loader (Databricks recommended)
def auto_loader_incremental():
    auto_loader_df = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("cloudFiles.inferSchema", "true") \
        .option("cloudFiles.schemaLocation", "/tmp/checkpoints/schema") \
        .load("/path/to/raw/ecommerce/data")
    
    query = auto_loader_df.writeStream \
        .outputMode("append") \
        .format("delta") \
        .option("checkpointLocation", "/tmp/checkpoints/auto_loader") \
        .trigger(availableNow=True) \
        .toTable("ecommerce_incremental_table")
    
    query.awaitTermination()

# Method 3: Using change data feed (Delta Lake)
def incremental_with_cdf():
    # Enable CDF on source table
    spark.sql("ALTER TABLE ecommerce_source SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    
    # Read changes since version
    changes_df = spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", 0) \
        .table("ecommerce_source")
    
    # Apply changes (upsert/delete) to target
    changes_df.createOrReplaceTempView("changes")
    
    # Merge into target (SCD1 or SCD2)
    spark.sql("""
        MERGE INTO ecommerce_target AS target
        USING changes AS source
        ON target.order_id = source.order_id
        WHEN MATCHED AND source._change_type = 'update' THEN UPDATE SET *
        WHEN MATCHED AND source._change_type = 'delete' THEN DELETE
        WHEN NOT MATCHED AND source._change_type = 'insert' THEN INSERT *
    """)