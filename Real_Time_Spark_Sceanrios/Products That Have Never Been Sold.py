from pyspark.sql.functions import col, broadcast

def unsold_products(products_df, sales_df):
    # Method 1: Left anti join (most efficient)
    unsold = products_df.join(
        sales_df.select("product_id").distinct(),
        "product_id",
        "left_anti"
    )

%sql
SELECT product_id, product_name
FROM products
WHERE product_id NOT IN (
    SELECT DISTINCT product_id
    FROM sales
    WHERE product_id IS NOT NULL);