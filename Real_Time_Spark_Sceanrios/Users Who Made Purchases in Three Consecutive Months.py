from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize Spark
spark = SparkSession.builder \
    .appName("ConsecutiveMonths") \
    .getOrCreate()

# Assuming you have a purchases DataFrame
# purchases_df = spark.read.table("purchases") or spark.read.parquet("path/to/purchases")

# Step 1: Get distinct user_id and purchase month
user_monthly_purchases = purchases_df.select(
    "user_id",
    trunc("purchase_date", "MM").alias("purchase_month")).distinct()

# Step 2: Add lag columns to check consecutive months
window_spec = Window.partitionBy("user_id").orderBy("purchase_month")

consecutive_check = user_monthly_purchases.withColumn(
    "prev_month", lag("purchase_month", 1).over(window_spec)).withColumn(
    "prev_2_month", lag("purchase_month", 2).over(window_spec))

# Step 3: Filter for users with 3 consecutive months
result = consecutive_check.filter(
    (col("purchase_month") == add_months(col("prev_month"), 1)) &
    (col("prev_month") == add_months(col("prev_2_month"), 1))).select("user_id").distinct()

# Show results
result.show()


%sql

WITH user_monthly_purchases AS (
SELECT DISTINCT
        user_id,
        TRUNC(purchase_date, 'MM') AS purchase_month FROM purchases),

consecutive_check AS (
    SELECT 
        user_id,
        purchase_month,
        LAG(purchase_month, 1) OVER (PARTITION BY user_id ORDER BY purchase_month) AS prev_month,
        LAG(purchase_month, 2) OVER (PARTITION BY user_id ORDER BY purchase_month) AS prev_2_month FROM user_monthly_purchases)

SELECT DISTINCT user_id FROM consecutive_check
WHERE purchase_month = ADD_MONTHS(prev_month, 1) AND prev_month = ADD_MONTHS(prev_2_month, 1);