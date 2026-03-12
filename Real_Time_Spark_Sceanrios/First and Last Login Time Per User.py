from pyspark.sql import Window
from pyspark.sql.functions import min, max, first, last, col

def get_user_login_times(login_events_df):
       login_events_df.write.partitionBy("user_id").bucketBy(100, "user_id").sortBy("login_time").saveAsTable("user_logins_bucketed")

       # Use aggregations for better performance at scale
    login_stats = spark.table("user_logins_bucketed") \
        .groupBy("user_id") \
        .agg(
            min("login_time").alias("first_login"),
            max("login_time").alias("last_login")
        )
    
    # Alternative using window functions with optimization
    window_spec = Window.partitionBy("user_id").orderBy("login_time")
    
    login_window_stats = spark.table("user_logins_bucketed") \
        .withColumn("first_login", first("login_time").over(window_spec)) \
        .withColumn("last_login", last("login_time").over(
            window_spec.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )) \
        .select("user_id", "first_login", "last_login") \
        .distinct()
    
    return login_stats 


%sql
SELECT 
    user_id,
    MIN(login_time) AS first_login,
    MAX(login_time) AS last_login FROM user_logins GROUP BY user_id;