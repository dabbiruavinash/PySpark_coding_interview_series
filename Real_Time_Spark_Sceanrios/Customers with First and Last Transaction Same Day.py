from pyspark.sql import Window
from pyspark.sql.functions import col, min, max, datediff, to_date

def same_day_first_last_transaction(transactions_df):
    # Get first and last transaction per customer
    customer_transactions = transactions_df.groupBy("customer_id") \
        .agg(
            min("transaction_date").alias("first_transaction"),
            max("transaction_date").alias("last_transaction")
        )
    
    # Filter where first and last are the same day
    same_day_customers = customer_transactions.filter(
        col("first_transaction") == col("last_transaction")
    )
    
    # Alternative: Check if customer has only one transaction
    transaction_counts = transactions_df.groupBy("customer_id") \
        .agg(count("*").alias("transaction_count"))
    
    single_transaction_customers = transaction_counts.filter(
        col("transaction_count") == 1
    ).join(
        transactions_df.select("customer_id", "transaction_date").distinct(),
        "customer_id"
    )
    
    # Detailed view with transaction info
    from pyspark.sql.functions import collect_list, struct
    
    customer_details = transactions_df.groupBy("customer_id") \
        .agg(
            min("transaction_date").alias("first_date"),
            max("transaction_date").alias("last_date"),
            count("*").alias("transaction_count"),
            collect_list(
                struct("transaction_id", "transaction_date", "amount")
            ).alias("transactions")
        ).filter(col("first_date") == col("last_date"))
    
    return same_day_customers

%sql
SELECT 
    customer_id,
    MIN(transaction_date) AS first_transaction,
    MAX(transaction_date) AS last_transaction FROM transactions GROUP BY customer_id HAVING MIN(transaction_date) = MAX(transaction_date);
