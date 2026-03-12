from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, rand, concat, lit, substring

def analyze_and_fix_skew_join(spark, large_df, small_df, join_key):
    # First, analyze the data distribution
    key_distribution = large_df.groupBy(join_key).count().orderBy(col("count").desc())
    
    # Show skewed keys
    skewed_keys = key_distribution.filter(col("count") > key_distribution.agg({"count": "avg"}).collect()[0][0] * 10)
    
    print("Skewed keys found:")
    skewed_keys.show()
    
    # Fix 1: Broadcast hint for small tables
    if small_df.count() < 10000000:  # 10M rows threshold
        fixed_join_1 = large_df.join(broadcast(small_df), join_key)
        print("Using broadcast join")
        return fixed_join_1
    
    # Fix 2: Salting for skewed keys
    if skewed_keys.count() > 0:
        # Add salt to large table
        salted_large = large_df.withColumn(
            "salt", 
            (rand() * 10).cast("int")
        ).withColumn(
            "salted_key",
            concat(col(join_key), lit("_"), col("salt"))
        )
        
        # Replicate small table with salt
        salted_small = small_df.crossJoin(
            spark.range(10).toDF("salt")
        ).withColumn(
            "salted_key",
            concat(col(join_key), lit("_"), col("salt"))
        )
        
        # Join on salted key
        fixed_join_2 = salted_large.join(salted_small, "salted_key") \
            .drop("salt", "salted_key")
        
        print("Using salting technique")
        return fixed_join_2
    
    # Fix 3: Repartition for better distribution
    fixed_join_3 = large_df.repartition(200, join_key) \
        .join(small_df.repartition(200, join_key), join_key)
    
    print("Using repartitioning")
    
    # Alternative: Analyze EXPLAIN plan programmatically
    def analyze_explain_plan(df):
        plan = df._jdf.queryExecution().toString()
        if "SortMergeJoin" in plan and "skew" in plan.lower():
            print("Potential skew detected in SortMergeJoin")
        if "BroadcastHashJoin" in plan:
            print("Using broadcast join - good for small tables")
        if "ShuffledHashJoin" in plan:
            print("Using shuffled hash join - check for data skew")
        return plan
    
    return fixed_join_3
