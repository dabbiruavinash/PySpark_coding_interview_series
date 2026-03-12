from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, first, datediff, current_date

def first_order_last_30_days(orders_df):
    # Get first order per user
    window_spec = Window.partitionBy("user_id").orderBy("order_date")
    
    first_orders = orders_df.withColumn(
        "rn", row_number().over(window_spec)
    ).filter(col("rn") == 1) \
     .select("user_id", "order_date".alias("first_order_date"))
    
    # Filter for last 30 days
    recent_first_orders = first_orders.filter(
        col("first_order_date") >= current_date() - 30
    )
    
    # Alternative using first_value
    first_orders_alt = orders_df.withColumn(
        "first_order_date",
        first("order_date").over(window_spec.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))).filter(
        col("order_date") == col("first_order_date")).select("user_id", "order_date".alias("first_order_date")) \
     .distinct() \
     .filter(col("first_order_date") >= current_date() - 30)
    
    return recent_first_orders

%sql
WITH user_first_orders AS (
    SELECT DISTINCT
        user_id,
        FIRST_VALUE(order_date) OVER (
            PARTITION BY user_id 
            ORDER BY order_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_order_date FROM orders)
SELECT user_id, first_order_date FROM user_first_orders WHERE first_order_date >= SYSDATE - 30;
