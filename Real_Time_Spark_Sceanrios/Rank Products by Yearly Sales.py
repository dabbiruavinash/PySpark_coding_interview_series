from pyspark.sql import Window
from pyspark.sql.functions import col, sum, rank, dense_rank, year, desc

def rank_products_yearly(sales_df):
    # Calculate yearly sales per product
    yearly_sales = sales_df.groupBy(
        year("sale_date").alias("year"),
        "product_id"
    ).agg(
        sum("revenue").alias("total_revenue"),
        sum("quantity").alias("total_quantity")
    )
    
    # Rank within each year
    window_spec = Window.partitionBy("year") \
                        .orderBy(col("total_revenue").desc(), col("product_id"))
    
    ranked_products = yearly_sales.withColumn(
        "rank", rank().over(window_spec)
    ).withColumn(
        "dense_rank", dense_rank().over(window_spec)
    ).orderBy("year", "rank")
    
    # For Delta Lake, partition by year
    ranked_products.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("year") \
        .saveAsTable("product_yearly_ranks")
    
    return ranked_products

%sql
WITH yearly_sales AS (
    SELECT 
        EXTRACT(YEAR FROM sale_date) AS year,
        product_id,
        SUM(revenue) AS total_revenue,
        SUM(quantity) AS total_quantity
    FROM sales
    GROUP BY EXTRACT(YEAR FROM sale_date), product_id
)
SELECT 
    year,
    product_id,
    total_revenue,
    total_quantity,
    RANK() OVER (PARTITION BY year ORDER BY total_revenue DESC) AS revenue_rank,
    DENSE_RANK() OVER (PARTITION BY year ORDER BY total_revenue DESC) AS revenue_dense_rank,
    RANK() OVER (PARTITION BY year ORDER BY total_quantity DESC) AS quantity_rank,
    PERCENT_RANK() OVER (PARTITION BY year ORDER BY total_revenue DESC) AS revenue_percentile
FROM yearly_sales
ORDER BY year, revenue_rank;