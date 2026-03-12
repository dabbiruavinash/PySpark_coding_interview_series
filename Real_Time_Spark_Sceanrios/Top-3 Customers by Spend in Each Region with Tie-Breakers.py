from pyspark.sql import Window
from pyspark.sql.functions import col, sum, row_number, rank, dense_rank, desc

def get_top_customers_by_region(sales_df):
       customer_spend = sales_df.groupBy("regioin", "customer_id").agg(sum("amount").alias("total_spend"))

       window_spec = Window.partitionBy("region").orderBy(col("total_spend").desc(), col("customer_id"))

       top_customers = customer_spend \
        .withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") <= 3) \
        .select("region", "customer_id", "total_spend", "rank")
    
    # Alternative: Use dense_rank if you want ties to share rank
    window_spec_ties = Window.partitionBy("region") \
                             .orderBy(col("total_spend").desc())
    
    top_customers_with_ties = customer_spend \
        .withColumn("dense_rank", dense_rank().over(window_spec_ties)) \
        .filter(col("dense_rank") <= 3) \
        .select("region", "customer_id", "total_spend", "dense_rank")
    
    return top_customers

%sql
-- Using ROW_NUMBER for unique ranking with tie-breaker
WITH customer_spend AS (
    SELECT 
        region,
        customer_id,
        SUM(amount) AS total_spend FROM sales GROUP BY region, customer_id),
ranked_customers AS (
    SELECT 
        region,
        customer_id,
        total_spend,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY total_spend DESC, customer_id) AS rank FROM customer_spend)
SELECT region, customer_id, total_spend, rank FROM ranked_customers WHERE rank <= 3;
