from pyspark.sql import Window
from pyspark.sql.functions import col, avg, sum, date_sub, to_date, when
from pyspark.sql.types import DateType

def compute_7day_moving_avg(sales_df):
    # Ensure date column is date type
    sales_df = sales_df.withColumn("sale_date", to_date("sale_date"))
    
    # Define window specification for last 7 days including current
    window_spec = Window.partitionBy("product_id") \
                        .orderBy("sale_date") \
                        .rowsBetween(-6, Window.currentRow)
    
    # Alternative: Use rangeBetween for exact 7-day window
    window_spec_range = Window.partitionBy("product_id") \
                              .orderBy("unix_date") \
                              .rangeBetween(-7 * 24 * 60 * 60, 0)
    
    # Calculate daily sales first
    daily_sales = sales_df.groupBy("product_id", "sale_date") \
                          .agg(sum("quantity").alias("daily_quantity"),
                               sum("revenue").alias("daily_revenue"))
    
    # Compute moving averages
    moving_avg_df = daily_sales.withColumn(
        "7day_avg_quantity",
        avg("daily_quantity").over(window_spec)).withColumn(
        "7day_avg_revenue",
        avg("daily_revenue").over(window_spec)).withColumn(
        "7day_sum_revenue",
        sum("daily_revenue").over(window_spec))
    
    # Handle incomplete windows (first 6 days)
    moving_avg_df = moving_avg_df.withColumn(
        "7day_avg_quantity",
        when(col("7day_avg_quantity").isNull(), col("daily_quantity"))
        .otherwise(col("7day_avg_quantity")))
    
    # For Delta Lake with time-travel
    from delta.tables import DeltaTable
    
    # Write with partitioning for better performance
    moving_avg_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("product_id") \
        .saveAsTable("product_moving_averages")
    
    return moving_avg_df

# Streaming version with watermarks
def streaming_7day_moving_avg(spark, stream_df):
    from pyspark.sql.functions import window
    
    streaming_avg = stream_df \
        .withWatermark("sale_timestamp", "7 days") \
        .groupBy(
            "product_id",
            window("sale_timestamp", "1 day")
        ) \
        .agg(
            sum("quantity").alias("daily_quantity"),
            sum("revenue").alias("daily_revenue")
        ) \
        .select(
            "product_id",
            col("window.start").alias("sale_date"),
            "daily_quantity",
            "daily_revenue"
        )
    
    return streaming_avg

%sql
SELECT 
    product_id,
    sale_date,
    daily_quantity,
    daily_revenue,
    AVG(daily_quantity) OVER (PARTITION BY product_id ORDER BY sale_date RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW) AS avg_quantity_7day_exact FROM daily_sales;