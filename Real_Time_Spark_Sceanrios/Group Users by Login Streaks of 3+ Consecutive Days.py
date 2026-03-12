from pyspark.sql import Window
from pyspark.sql.functions import col, lag, datediff, sum, row_number, date_sub
from pyspark.sql.types import IntegerType

def find_login_streaks(logins_df, min_streak_days=3):
    # Ensure we have distinct daily logins per user
    daily_logins = logins_df.select(
        "user_id",
        col("login_date").cast("date").alias("login_date")
    ).distinct()
    
    # Window spec for each user ordered by date
    window_spec = Window.partitionBy("user_id").orderBy("login_date")
    
    # Find gaps between logins
    logins_with_gaps = daily_logins.withColumn(
        "prev_date", lag("login_date", 1).over(window_spec)
    ).withColumn(
        "day_diff",
        datediff(col("login_date"), col("prev_date"))
    )
    
    # Create streak groups (new group when gap > 1 day)
    logins_with_groups = logins_with_gaps.withColumn(
        "new_streak",
        (col("day_diff") > 1).cast("int")
    ).withColumn(
        "streak_group",
        sum("new_streak").over(
            window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
    ).fillna(0, subset=["streak_group"])
    
    # Calculate streak lengths
    streak_lengths = logins_with_groups.groupBy(
        "user_id", "streak_group"
    ).agg(
        count("*").alias("streak_days"),
        min("login_date").alias("streak_start"),
        max("login_date").alias("streak_end")
    ).filter(col("streak_days") >= min_streak_days)
    
    # Alternative method using date differences
    # Create a row number and subtract days to find consecutive groups
    logins_with_rn = daily_logins.withColumn(
        "rn", row_number().over(window_spec)).withColumn(
        "date_minus_rn",
        date_sub(col("login_date"), col("rn")))
    
    streaks_alt = logins_with_rn.groupBy(
        "user_id", "date_minus_rn").agg(
        count("*").alias("streak_days"),
        min("login_date").alias("streak_start"),
        max("login_date").alias("streak_end")).filter(col("streak_days") >= min_streak_days)
    
    return streak_lengths

%sql
WITH daily_logins AS (
    SELECT DISTINCT
        user_id,
        TRUNC(login_date) AS login_date
    FROM user_logins
),
login_groups AS (
    SELECT 
        user_id,
        login_date,
        login_date - ROW_NUMBER() OVER (
            PARTITION BY user_id 
            ORDER BY login_date
        ) AS group_id
    FROM daily_logins)
SELECT 
    user_id,
    MIN(login_date) AS streak_start,
    MAX(login_date) AS streak_end,
    COUNT(*) AS streak_days FROM login_groups GROUP BY user_id, group_id HAVING COUNT(*) >= 3 ORDER BY user_id, streak_start;
