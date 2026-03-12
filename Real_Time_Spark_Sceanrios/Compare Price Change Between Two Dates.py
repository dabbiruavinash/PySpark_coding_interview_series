from pyspark.sql import Window
from pyspark.sql.functions import col, lag, when, datediff, lit

def price_change_comparison(prices_df, date1, date2):
    # Method 1: Self-join
    prices_date1 = prices_df.filter(col("price_date") == date1) \
        .selectExpr("product_id as product_id", "price as price_1")
    
    prices_date2 = prices_df.filter(col("price_date") == date2) \
        .selectExpr("product_id as product_id", "price as price_2")
    
    price_comparison = prices_date1.join(prices_date2, "product_id", "full_outer") \
        .withColumn("price_change", col("price_2") - col("price_1")) \
        .withColumn("price_change_pct", 
                    when(col("price_1") != 0, 
                         (col("price_2") - col("price_1")) / col("price_1") * 100)
                    .otherwise(None))
    
%sql

WITH filtered_prices AS (
    SELECT 
        product_id,
        price_date,
        price,
        LAG(price) OVER (PARTITION BY product_id ORDER BY price_date) AS prev_price,
        LAG(price_date) OVER (PARTITION BY product_id ORDER BY price_date) AS prev_date
    FROM product_prices
    WHERE price_date IN (DATE '2024-01-01', DATE '2024-02-01')
)
SELECT 
    product_id,
    prev_date AS date1,
    price_date AS date2,
    prev_price AS price1,
    price AS price2,
    price - prev_price AS price_change,
    CASE 
        WHEN prev_price > 0 THEN ((price - prev_price) / prev_price) * 100
        ELSE NULL
    END AS percent_change
FROM filtered_prices
WHERE prev_date = DATE '2024-01-01' AND price_date = DATE '2024-02-01';
