 grouped_median = transactions_df.groupBy("group_column") \
        .agg(percentile_approx(amount_col, 0.5, 10000).alias("median_amount"))
    
    return median_exact

# For streaming applications
def streaming_median(spark, stream_df):
    from pyspark.sql.functions import window, approxQuantile
    
    # Use t-digest or approxQuantile for streaming
    streaming_median = stream_df \
        .groupBy(window("timestamp", "1 hour")) \
        .agg(percentile_approx("amount", 0.5, 10000).alias("median_amount"))
    
    return streaming_median

%sql
SELECT MEDIAN(amount) AS median_amount FROM transactions;