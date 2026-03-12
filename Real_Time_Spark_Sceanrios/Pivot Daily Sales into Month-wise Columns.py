from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, month, year, expr, first, last
from pyspark.sql.types import IntegerType

def pivot_daily_to_monthly(sales_df):
    # Extract month and year
    sales_with_month = sales_df.withColumn(
        "year", year("sale_date")
    ).withColumn(
        "month", month("sale_date")
    )
    
    # Get distinct months for pivoting
    months = [row[0] for row in 
              sales_with_month.select("month").distinct().orderBy("month").collect()]
    
    # Create month names for columns
    month_names = [f"month_{m:02d}" for m in months]
    
    # Pivot the data
    pivoted_df = sales_with_month.groupBy("product_id", "year") \
        .pivot("month", months) \
        .agg(sum("revenue").alias("revenue")) \
        .fillna(0)
    
    # Rename columns for clarity
    for i, month in enumerate(months):
        pivoted_df = pivoted_df.withColumnRenamed(
            str(month), 
            f"month_{month:02d}_revenue"
        )
    
    # Alternative: Using case when for streaming compatibility
    from pyspark.sql.functions import when
    
    streaming_friendly = sales_with_month.groupBy("product_id", "year") \
        .agg(
            *[sum(when(col("month") == m, col("revenue")).otherwise(0)).alias(f"month_{m:02d}")
              for m in range(1, 13)]
        )
    
    # For streaming, use foreachBatch with merge
    def write_pivot_to_delta(df, epoch_id):
        from delta.tables import DeltaTable
        
        if DeltaTable.isDeltaTable(spark, "monthly_pivoted_sales"):
            delta_table = DeltaTable.forName(spark, "monthly_pivoted_sales")
            
            # Merge logic for upsert
            delta_table.alias("target") \
                .merge(
                    df.alias("source"),
                    "target.product_id = source.product_id AND target.year = source.year"
                ) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
        else:
            df.write.format("delta").saveAsTable("monthly_pivoted_sales")
    
    # Apply to streaming
    # streaming_df.writeStream.foreachBatch(write_pivot_to_delta).start()
    
    return pivoted_df

%sql
SELECT * FROM (
    SELECT 
        product_id,
        TO_CHAR(sale_date, 'YYYY') AS year,
        TO_CHAR(sale_date, 'MM') AS month,
        revenue
    FROM sales
)
PIVOT (
    SUM(revenue) AS revenue
    FOR month IN (
        '01' AS JAN, '02' AS FEB, '03' AS MAR,
        '04' AS APR, '05' AS MAY, '06' AS JUN,
        '07' AS JUL, '08' AS AUG, '09' AS SEP,
        '10' AS OCT, '11' AS NOV, '12' AS DEC)) ORDER BY product_id, year;