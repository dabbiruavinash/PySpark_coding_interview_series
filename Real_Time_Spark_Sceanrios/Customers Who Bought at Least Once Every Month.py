from pyspark.sql import Window
from pyspark.sql.functions import col, month, year, count, min, max, datediff, when

def customers_every_month(purchases_df, target_year):
    # Get distinct customer-months
    customer_months = purchases_df.filter(year("purchase_date") == target_year) \
        .select(
            "customer_id",
            month("purchase_date").alias("month")
        ).distinct()
    
    # Count months per customer
    months_per_customer = customer_months.groupBy("customer_id") \
        .agg(count("*").alias("months_with_purchase"))
    
    # Customers who purchased in all 12 months
    full_year_customers = months_per_customer.filter(col("months_with_purchase") == 12) \
        .select("customer_id")
    
    # Alternative: Use collect_set approach
    from pyspark.sql.functions import collect_set, size
    
    customer_month_set = purchases_df.filter(year("purchase_date") == target_year) \
        .groupBy("customer_id") \
        .agg(collect_set(month("purchase_date")).alias("months_purchased")) \
        .withColumn("month_count", size("months_purchased")) \
        .filter(col("month_count") == 12)
    
    # Verify all months 1-12 are present
    from pyspark.sql.functions import array, array_contains
    
    all_months = array([lit(m) for m in range(1, 13)])
    
    customer_all_months = customer_month_set.filter(
        col("months_purchased") == all_months
    )
    
    return full_year_customers

%sql
SELECT 
    customer_id
FROM purchases
WHERE EXTRACT(YEAR FROM purchase_date) = 2024
GROUP BY customer_id
HAVING COUNT(DISTINCT EXTRACT(MONTH FROM purchase_date)) = 12;