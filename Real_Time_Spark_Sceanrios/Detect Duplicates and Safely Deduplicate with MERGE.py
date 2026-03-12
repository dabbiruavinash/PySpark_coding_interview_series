from delta.tables import DeltaTable
from pyspark.sql.functions import col, count, row_number
from pyspark.sql import Window

def detect_and_duplicate(spark, table_name, key_columns):
       delta_table = DeltaTable.forName(spark, table_name)
       df = detla_table.toDF()

       # detect duplicates
        duplicate_check = df.groupBy(key_columns).agg(count("*").alias("dup_count")).filter(col("dup_count") > 1)

        duplicate_count = duplicate_check.count()
        print(f"Found {duplicate_count} duplicate groups")

        if duplicate_count > 0:
        # Create deduplicated view using row_number
        window_spec = Window.partitionBy(key_columns).orderBy(col("last_updated").desc())
        
        deduped_df = df.withColumn("rn", row_number().over(window_spec)) \
                       .filter(col("rn") == 1) \
                       .drop("rn")

         # Perform idempotent merge to replace duplicates
        delta_table.alias("target") \
            .merge(
                deduped_df.alias("source"),
                " AND ".join([f"target.{k} = source.{k}" for k in key_columns])
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        
        # Alternative: Delete duplicates then insert clean data
        # Create temp view of keys to keep
        keys_to_keep = df.withColumn("rn", row_number().over(window_spec)) \
                         .filter(col("rn") == 1) \
                         .select(key_columns)
        
        # Delete duplicates
        from pyspark.sql.functions import broadcast
        duplicate_keys = df.join(broadcast(keys_to_keep), key_columns, "left_anti") \
                           .select(key_columns).distinct()
        
        # Perform delete
        delta_table.alias("target") \
            .merge(
                duplicate_keys.alias("source"),
                " AND ".join([f"target.{k} = source.{k}" for k in key_columns])
            ) \
            .whenMatchedDelete() \
            .execute()
    
    return duplicate_count

%sql
-- Detect duplicates
WITH duplicate_check AS (
    SELECT 
        key_column1,
        key_column2,
        COUNT(*) AS duplicate_count FROM transactions GROUP BY key_column1, key_column2 HAVING COUNT(*) > 1)
SELECT * FROM duplicate_check;

-- Safe deduplication using MERGE
MERGE INTO transactions target
USING (
    -- Keep only the most recent record per key
    SELECT 
        key_column1,
        key_column2,
        amount,
        transaction_date,
        last_updated,
        ROW_NUMBER() OVER (PARTITION BY key_column1, key_column2 ORDER BY last_updated DESC, transaction_date DESC) AS rn FROM transactions) source
ON (target.key_column1 = source.key_column1 
    AND target.key_column2 = source.key_column2
    AND source.rn = 1)
WHEN MATCHED THEN
    UPDATE SET 
        amount = source.amount,
        transaction_date = source.transaction_date,
        last_updated = SYSTIMESTAMP WHERE source.rn = 1

-- Delete duplicates
DELETE WHERE (target.key_column1, target.key_column2) IN (
    SELECT key_column1, key_column2
    FROM (
        SELECT 
            key_column1,
            key_column2,
            ROW_NUMBER() OVER (PARTITION BY key_column1, key_column2 ORDER BY last_updated DESC) AS rn FROM transactions) WHERE rn > 1);