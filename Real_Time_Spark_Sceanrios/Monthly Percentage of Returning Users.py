from pyspark.sql import Window
from pyspark.sql.functions import col, month, year, datediff, count, sum, first, min

def monthly_returning_users(transactions_df):
    # Get first purchase month for each user (cohort)
    first_purchase = transactions_df.groupBy("user_id") \
        .agg(
            min("transaction_date").alias("cohort_date")
        ).withColumn(
            "cohort_month",
            month("cohort_date")
        ).withColumn(
            "cohort_year",
            year("cohort_date")
        )
    
    # Join with transactions to get all purchases with cohort info
    user_cohorts = transactions_df.join(first_purchase, "user_id")
    
    # Create cohort month and transaction month
    user_cohorts = user_cohorts.withColumn(
        "transaction_month",
        month("transaction_date")
    ).withColumn(
        "transaction_year",
        year("transaction_date")
    ).withColumn(
        "months_since_first",
        ((col("transaction_year") - col("cohort_year")) * 12) +
        (col("transaction_month") - col("cohort_month"))
    )
    
    # Calculate cohort size (users who made first purchase in each month)
    cohort_sizes = first_purchase.groupBy("cohort_year", "cohort_month") \
        .agg(count("*").alias("cohort_size"))
    
    # Calculate monthly retention
    retention = user_cohorts.groupBy(
        "cohort_year", "cohort_month", "months_since_first"
    ).agg(
        countDistinct("user_id").alias("returning_users")
    ).join(cohort_sizes, ["cohort_year", "cohort_month"]) \
     .withColumn(
         "retention_rate",
         (col("returning_users") / col("cohort_size") * 100)
     ).orderBy("cohort_year", "cohort_month", "months_since_first")
    
    # Pivot for cohort table view
    cohort_table = retention.groupBy("cohort_year", "cohort_month") \
        .pivot("months_since_first") \
        .agg(first("retention_rate")) \
        .fillna(0)
    
    return retention, cohort_table

%sql
-- Cohort analysis with monthly retention
WITH user_cohorts AS (
    SELECT 
        user_id,
        TRUNC(MIN(transaction_date), 'MM') AS cohort_month
    FROM transactions
    GROUP BY user_id
),
cohort_size AS (
    SELECT 
        cohort_month,
        COUNT(*) AS cohort_size
    FROM user_cohorts
    GROUP BY cohort_month
),
user_activity AS (
    SELECT 
        uc.user_id,
        uc.cohort_month,
        TRUNC(t.transaction_date, 'MM') AS activity_month,
        MONTHS_BETWEEN(TRUNC(t.transaction_date, 'MM'), uc.cohort_month) AS month_number
    FROM user_cohorts uc
    JOIN transactions t ON uc.user_id = t.user_id
)
SELECT 
    ua.cohort_month,
    ua.month_number,
    COUNT(DISTINCT ua.user_id) AS active_users,
    cs.cohort_size,
    ROUND(COUNT(DISTINCT ua.user_id) * 100.0 / cs.cohort_size, 2) AS retention_pct
FROM user_activity ua
JOIN cohort_size cs ON ua.cohort_month = cs.cohort_month
GROUP BY ua.cohort_month, ua.month_number, cs.cohort_size
ORDER BY ua.cohort_month, ua.month_number;

-- Pivoted cohort table
SELECT * FROM (
    SELECT 
        TO_CHAR(cohort_month, 'YYYY-MM') AS cohort,
        month_number,
        retention_pct
    FROM (
        -- Previous query as subquery
        WITH ... -- (same as above)
    )
)
PIVOT (
    MAX(retention_pct)
    FOR month_number IN (0 AS "M0", 1 AS "M1", 2 AS "M2", 3 AS "M3", 4 AS "M4", 5 AS "M5")
)
ORDER BY cohort;