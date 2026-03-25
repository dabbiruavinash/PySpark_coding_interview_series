# Databricks PySpark code to flatten nested JSON

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, explode_outer, from_json, to_json, struct
from pyspark.sql.functions import lit, when, coalesce, arrays_zip, posexplode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, MapType
import json

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Ecommerce Flattening") \
    .getOrCreate()

# Read the JSON file
json_file_path = "/path/to/ecommerce.json"
df_raw = spark.read.option("multiline", "true").json(json_file_path)

# Extract the main data
df_ecommerce = df_raw.select("ecommerce_data")

# ============================================
# 1. FLATTEN CATEGORIES
# ============================================
categories_df = df_ecommerce.select(explode("ecommerce_data.categories").alias("category")) \
    .select(
        col("category.id").alias("category_id"),
        col("category.name").alias("category_name"),
        col("category.parent_id").alias("parent_category_id")
    )

# ============================================
# 2. FLATTEN PRODUCTS
# ============================================
products_df = df_ecommerce.select(explode("ecommerce_data.products").alias("product")) \
    .select(
        col("product.id").alias("product_id"),
        col("product.name").alias("product_name"),
        col("product.description").alias("product_description"),
        col("product.price").alias("price"),
        col("product.compare_at_price").alias("compare_at_price"),
        col("product.sku").alias("sku"),
        col("product.category_id").alias("category_id"),
        col("product.brand").alias("brand"),
        col("product.stock_quantity").alias("stock_quantity"),
        col("product.weight").alias("weight_kg"),
        col("product.dimensions.length").alias("dimension_length_cm"),
        col("product.dimensions.width").alias("dimension_width_cm"),
        col("product.dimensions.height").alias("dimension_height_cm"),
        col("product.status").alias("product_status"),
        col("product.created_at").alias("product_created_at"),
        concat_ws(",", col("product.tags")).alias("tags"),
        concat_ws(",", col("product.images")).alias("images")
    )

# ============================================
# 3. FLATTEN USERS WITH ADDRESSES
# ============================================
# First, explode addresses for each user
users_df = df_ecommerce.select(explode("ecommerce_data.users").alias("user")) \
    .select(
        col("user.id").alias("user_id"),
        col("user.email").alias("email"),
        col("user.first_name").alias("first_name"),
        col("user.last_name").alias("last_name"),
        col("user.phone").alias("phone"),
        col("user.created_at").alias("user_created_at"),
        col("user.status").alias("user_status"),
        explode_outer(col("user.addresses")).alias("address")
    ) \
    .select(
        "user_id",
        "email",
        "first_name",
        "last_name",
        "phone",
        "user_created_at",
        "user_status",
        col("address.type").alias("address_type"),
        col("address.street").alias("street"),
        col("address.city").alias("city"),
        col("address.state").alias("state"),
        col("address.zip_code").alias("zip_code"),
        col("address.country").alias("country")
    )

# ============================================
# 4. FLATTEN ORDERS WITH ITEMS
# ============================================
# First explode order items, then join with order details
orders_df = df_ecommerce.select(explode("ecommerce_data.orders").alias("order")) \
    .select(
        col("order.id").alias("order_id"),
        col("order.user_id").alias("user_id"),
        col("order.order_date").alias("order_date"),
        col("order.status").alias("order_status"),
        col("order.subtotal").alias("subtotal"),
        col("order.shipping_cost").alias("shipping_cost"),
        col("order.tax").alias("tax"),
        col("order.total").alias("total_amount"),
        col("order.payment_method").alias("payment_method"),
        col("order.tracking_number").alias("tracking_number"),
        explode_outer(col("order.items")).alias("item"),
        col("order.shipping_address.street").alias("shipping_street"),
        col("order.shipping_address.city").alias("shipping_city"),
        col("order.shipping_address.state").alias("shipping_state"),
        col("order.shipping_address.zip_code").alias("shipping_zip"),
        col("order.shipping_address.country").alias("shipping_country")
    ) \
    .select(
        "order_id",
        "user_id",
        "order_date",
        "order_status",
        "subtotal",
        "shipping_cost",
        "tax",
        "total_amount",
        "payment_method",
        "tracking_number",
        col("item.product_id").alias("product_id"),
        col("item.quantity").alias("quantity"),
        col("item.price_at_time").alias("price_at_time"),
        "shipping_street",
        "shipping_city",
        "shipping_state",
        "shipping_zip",
        "shipping_country"
    )

# ============================================
# 5. FLATTEN REVIEWS
# ============================================
reviews_df = df_ecommerce.select(explode("ecommerce_data.reviews").alias("review")) \
    .select(
        col("review.id").alias("review_id"),
        col("review.product_id").alias("product_id"),
        col("review.user_id").alias("user_id"),
        col("review.rating").alias("rating"),
        col("review.title").alias("review_title"),
        col("review.comment").alias("review_comment"),
        col("review.created_at").alias("review_created_at"),
        col("review.verified_purchase").alias("verified_purchase"),
        col("review.helpful_votes").alias("helpful_votes")
    )

# ============================================
# 6. FLATTEN INVENTORY UPDATES
# ============================================
inventory_df = df_ecommerce.select(explode("ecommerce_data.inventory_updates").alias("inventory")) \
    .select(
        col("inventory.id").alias("inventory_id"),
        col("inventory.product_id").alias("product_id"),
        col("inventory.change_quantity").alias("change_quantity"),
        col("inventory.new_quantity").alias("new_quantity"),
        col("inventory.reason").alias("change_reason"),
        col("inventory.date").alias("change_date")
    )

# ============================================
# 7. FLATTEN PROMOTIONS
# ============================================
promotions_df = df_ecommerce.select(explode("ecommerce_data.promotions").alias("promotion")) \
    .select(
        col("promotion.id").alias("promotion_id"),
        col("promotion.name").alias("promotion_name"),
        col("promotion.code").alias("promo_code"),
        col("promotion.discount_type").alias("discount_type"),
        col("promotion.discount_value").alias("discount_value"),
        col("promotion.start_date").alias("promo_start_date"),
        col("promotion.end_date").alias("promo_end_date"),
        col("promotion.min_purchase").alias("min_purchase_amount"),
        col("promotion.active").alias("is_active"),
        concat_ws(",", col("promotion.applicable_categories")).alias("applicable_categories")
    )

# ============================================
# 8. FLATTEN ANALYTICS
# ============================================
analytics_df = df_ecommerce.select("ecommerce_data.analytics") \
    .select(
        col("analytics.total_products").alias("total_products"),
        col("analytics.total_users").alias("total_users"),
        col("analytics.total_orders").alias("total_orders"),
        col("analytics.total_revenue").alias("total_revenue"),
        col("analytics.average_order_value").alias("average_order_value"),
        explode_outer(col("analytics.top_selling_products")).alias("top_product")
    ) \
    .select(
        "total_products",
        "total_users",
        "total_orders",
        "total_revenue",
        "average_order_value",
        col("top_product.product_id").alias("top_product_id"),
        col("top_product.units_sold").alias("top_product_units_sold"),
        col("top_product.revenue").alias("top_product_revenue")
    )

# ============================================
# DISPLAY RESULTS
# ============================================
print("=== CATEGORIES ===")
categories_df.show(truncate=False)

print("=== PRODUCTS ===")
products_df.show(5, truncate=False)

print("=== USERS WITH ADDRESSES ===")
users_df.show(truncate=False)

print("=== ORDERS WITH ITEMS ===")
orders_df.show(5, truncate=False)

print("=== REVIEWS ===")
reviews_df.show(truncate=False)

print("=== INVENTORY ===")
inventory_df.show(truncate=False)

print("=== PROMOTIONS ===")
promotions_df.show(truncate=False)

# ============================================
# SAVE FLATTENED TABLES TO DELTA FORMAT
# ============================================
categories_df.write.mode("overwrite").format("delta").save("/mnt/datalake/ecommerce_flattened/categories")
products_df.write.mode("overwrite").format("delta").save("/mnt/datalake/ecommerce_flattened/products")
users_df.write.mode("overwrite").format("delta").save("/mnt/datalake/ecommerce_flattened/users")
orders_df.write.mode("overwrite").format("delta").save("/mnt/datalake/ecommerce_flattened/orders")
reviews_df.write.mode("overwrite").format("delta").save("/mnt/datalake/ecommerce_flattened/reviews")
inventory_df.write.mode("overwrite").format("delta").save("/mnt/datalake/ecommerce_flattened/inventory")
promotions_df.write.mode("overwrite").format("delta").save("/mnt/datalake/ecommerce_flattened/promotions")

# Create temporary views for SQL queries
categories_df.createOrReplaceTempView("categories")
products_df.createOrReplaceTempView("products")
users_df.createOrReplaceTempView("users")
orders_df.createOrReplaceTempView("orders")
reviews_df.createOrReplaceTempView("reviews")

# Example: Join orders with products to get detailed order information
order_details = spark.sql("""
    SELECT 
        o.order_id,
        o.user_id,
        u.first_name,
        u.last_name,
        o.order_date,
        o.order_status,
        o.product_id,
        p.product_name,
        o.quantity,
        o.price_at_time,
        (o.quantity * o.price_at_time) as line_total,
        o.total_amount
    FROM orders o
    LEFT JOIN products p ON o.product_id = p.product_id
    LEFT JOIN users u ON o.user_id = u.user_id
    ORDER BY o.order_date DESC
""")

print("=== ORDER DETAILS (JOINED) ===")
order_details.show(10, truncate=False)

# Snowflake Approach 

-- Snowflake SQL to flatten nested JSON

-- Create file format for JSON
CREATE OR REPLACE FILE FORMAT ecommerce_json_format
    TYPE = JSON
    STRIP_OUTER_ARRAY = TRUE
    ALLOW_DUPLICATE = TRUE;

-- Create stage for JSON file
CREATE OR REPLACE STAGE ecommerce_stage
    FILE_FORMAT = ecommerce_json_format;

-- Assuming the JSON file is uploaded to the stage
-- PUT file://ecommerce.json @ecommerce_stage;

-- Create a table to store the raw JSON
CREATE OR REPLACE TABLE raw_ecommerce_json (
    raw_data VARIANT
);

-- Copy JSON data into the table
COPY INTO raw_ecommerce_json
FROM @ecommerce_stage/ecommerce.json
ON_ERROR = 'CONTINUE';

-- ============================================
-- 1. FLATTEN CATEGORIES
-- ============================================
CREATE OR REPLACE VIEW flattened_categories AS
SELECT 
    f.value:id::INTEGER AS category_id,
    f.value:name::STRING AS category_name,
    f.value:parent_id::INTEGER AS parent_category_id
FROM raw_ecommerce_json,
LATERAL FLATTEN(input => raw_data:ecommerce_data.categories) f;

-- ============================================
-- 2. FLATTEN PRODUCTS
-- ============================================
CREATE OR REPLACE VIEW flattened_products AS
SELECT 
    f.value:id::STRING AS product_id,
    f.value:name::STRING AS product_name,
    f.value:description::STRING AS product_description,
    f.value:price::DECIMAL(10,2) AS price,
    f.value:compare_at_price::DECIMAL(10,2) AS compare_at_price,
    f.value:sku::STRING AS sku,
    f.value:category_id::INTEGER AS category_id,
    f.value:brand::STRING AS brand,
    f.value:stock_quantity::INTEGER AS stock_quantity,
    f.value:weight::DECIMAL(10,2) AS weight_kg,
    f.value:dimensions:length::DECIMAL(10,2) AS dimension_length_cm,
    f.value:dimensions:width::DECIMAL(10,2) AS dimension_width_cm,
    f.value:dimensions:height::DECIMAL(10,2) AS dimension_height_cm,
    f.value:status::STRING AS product_status,
    f.value:created_at::TIMESTAMP AS product_created_at,
    ARRAY_TO_STRING(f.value:tags, ',') AS tags,
    ARRAY_TO_STRING(f.value:images, ',') AS images
FROM raw_ecommerce_json,
LATERAL FLATTEN(input => raw_data:ecommerce_data.products) f;

-- ============================================
-- 3. FLATTEN USERS WITH ADDRESSES
-- ============================================
CREATE OR REPLACE VIEW flattened_users AS
SELECT 
    u.value:id::STRING AS user_id,
    u.value:email::STRING AS email,
    u.value:first_name::STRING AS first_name,
    u.value:last_name::STRING AS last_name,
    u.value:phone::STRING AS phone,
    u.value:created_at::TIMESTAMP AS user_created_at,
    u.value:status::STRING AS user_status,
    a.value:type::STRING AS address_type,
    a.value:street::STRING AS street,
    a.value:city::STRING AS city,
    a.value:state::STRING AS state,
    a.value:zip_code::STRING AS zip_code,
    a.value:country::STRING AS country
FROM raw_ecommerce_json,
LATERAL FLATTEN(input => raw_data:ecommerce_data.users) u,
LATERAL FLATTEN(input => u.value:addresses) a;

-- ============================================
-- 4. FLATTEN ORDERS WITH ITEMS
-- ============================================
CREATE OR REPLACE VIEW flattened_orders AS
SELECT 
    o.value:id::STRING AS order_id,
    o.value:user_id::STRING AS user_id,
    o.value:order_date::TIMESTAMP AS order_date,
    o.value:status::STRING AS order_status,
    o.value:subtotal::DECIMAL(10,2) AS subtotal,
    o.value:shipping_cost::DECIMAL(10,2) AS shipping_cost,
    o.value:tax::DECIMAL(10,2) AS tax,
    o.value:total::DECIMAL(10,2) AS total_amount,
    o.value:payment_method::STRING AS payment_method,
    o.value:tracking_number::STRING AS tracking_number,
    i.value:product_id::STRING AS product_id,
    i.value:quantity::INTEGER AS quantity,
    i.value:price_at_time::DECIMAL(10,2) AS price_at_time,
    o.value:shipping_address:street::STRING AS shipping_street,
    o.value:shipping_address:city::STRING AS shipping_city,
    o.value:shipping_address:state::STRING AS shipping_state,
    o.value:shipping_address:zip_code::STRING AS shipping_zip,
    o.value:shipping_address:country::STRING AS shipping_country
FROM raw_ecommerce_json,
LATERAL FLATTEN(input => raw_data:ecommerce_data.orders) o,
LATERAL FLATTEN(input => o.value:items) i;

-- ============================================
-- 5. FLATTEN REVIEWS
-- ============================================
CREATE OR REPLACE VIEW flattened_reviews AS
SELECT 
    f.value:id::STRING AS review_id,
    f.value:product_id::STRING AS product_id,
    f.value:user_id::STRING AS user_id,
    f.value:rating::INTEGER AS rating,
    f.value:title::STRING AS review_title,
    f.value:comment::STRING AS review_comment,
    f.value:created_at::TIMESTAMP AS review_created_at,
    f.value:verified_purchase::BOOLEAN AS verified_purchase,
    f.value:helpful_votes::INTEGER AS helpful_votes
FROM raw_ecommerce_json,
LATERAL FLATTEN(input => raw_data:ecommerce_data.reviews) f;

-- ============================================
-- 6. FLATTEN INVENTORY UPDATES
-- ============================================
CREATE OR REPLACE VIEW flattened_inventory AS
SELECT 
    f.value:id::STRING AS inventory_id,
    f.value:product_id::STRING AS product_id,
    f.value:change_quantity::INTEGER AS change_quantity,
    f.value:new_quantity::INTEGER AS new_quantity,
    f.value:reason::STRING AS change_reason,
    f.value:date::TIMESTAMP AS change_date
FROM raw_ecommerce_json,
LATERAL FLATTEN(input => raw_data:ecommerce_data.inventory_updates) f;

-- ============================================
-- 7. FLATTEN PROMOTIONS
-- ============================================
CREATE OR REPLACE VIEW flattened_promotions AS
SELECT 
    f.value:id::STRING AS promotion_id,
    f.value:name::STRING AS promotion_name,
    f.value:code::STRING AS promo_code,
    f.value:discount_type::STRING AS discount_type,
    f.value:discount_value::INTEGER AS discount_value,
    f.value:start_date::TIMESTAMP AS promo_start_date,
    f.value:end_date::TIMESTAMP AS promo_end_date,
    f.value:min_purchase::DECIMAL(10,2) AS min_purchase_amount,
    f.value:active::BOOLEAN AS is_active,
    ARRAY_TO_STRING(f.value:applicable_categories, ',') AS applicable_categories
FROM raw_ecommerce_json,
LATERAL FLATTEN(input => raw_data:ecommerce_data.promotions) f;

-- ============================================
-- 8. FLATTEN ANALYTICS
-- ============================================
CREATE OR REPLACE VIEW flattened_analytics AS
SELECT 
    raw_data:ecommerce_data.analytics.total_products::INTEGER AS total_products,
    raw_data:ecommerce_data.analytics.total_users::INTEGER AS total_users,
    raw_data:ecommerce_data.analytics.total_orders::INTEGER AS total_orders,
    raw_data:ecommerce_data.analytics.total_revenue::DECIMAL(10,2) AS total_revenue,
    raw_data:ecommerce_data.analytics.average_order_value::DECIMAL(10,2) AS average_order_value,
    t.value:product_id::STRING AS top_product_id,
    t.value:units_sold::INTEGER AS top_product_units_sold,
    t.value:revenue::DECIMAL(10,2) AS top_product_revenue
FROM raw_ecommerce_json,
LATERAL FLATTEN(input => raw_data:ecommerce_data.analytics.top_selling_products) t;

-- ============================================
-- BUSINESS INTELLIGENCE QUERIES
-- ============================================

-- 1. Product performance with ratings
SELECT 
    p.product_id,
    p.product_name,
    p.brand,
    p.price,
    p.stock_quantity,
    COALESCE(AVG(r.rating), 0) AS avg_rating,
    COUNT(r.review_id) AS review_count,
    SUM(o.quantity) AS total_units_sold,
    SUM(o.quantity * o.price_at_time) AS total_revenue
FROM flattened_products p
LEFT JOIN flattened_orders o ON p.product_id = o.product_id
LEFT JOIN flattened_reviews r ON p.product_id = r.product_id
GROUP BY p.product_id, p.product_name, p.brand, p.price, p.stock_quantity
ORDER BY total_revenue DESC NULLS LAST;

-- 2. Customer purchase history
SELECT 
    u.user_id,
    u.email,
    u.first_name,
    u.last_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.total_amount) AS total_spent,
    AVG(o.total_amount) AS avg_order_value,
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date
FROM flattened_users u
LEFT JOIN flattened_orders o ON u.user_id = o.user_id
GROUP BY u.user_id, u.email, u.first_name, u.last_name
ORDER BY total_spent DESC;

-- 3. Monthly sales trend
SELECT 
    DATE_TRUNC('month', order_date) AS month,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value,
    COUNT(DISTINCT user_id) AS unique_customers
FROM flattened_orders
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month DESC;

-- 4. Inventory turnover analysis
SELECT 
    p.product_id,
    p.product_name,
    p.stock_quantity AS current_stock,
    COALESCE(SUM(o.quantity), 0) AS units_sold,
    CASE 
        WHEN COALESCE(SUM(o.quantity), 0) = 0 THEN 0
        ELSE p.stock_quantity / SUM(o.quantity) 
    END AS stock_to_sales_ratio
FROM flattened_products p
LEFT JOIN flattened_orders o ON p.product_id = o.product_id
GROUP BY p.product_id, p.product_name, p.stock_quantity
ORDER BY stock_to_sales_ratio ASC;

-- 5. Category performance
SELECT 
    c.category_id,
    c.category_name,
    COUNT(DISTINCT p.product_id) AS product_count,
    SUM(o.quantity) AS total_units_sold,
    SUM(o.quantity * o.price_at_time) AS total_revenue,
    AVG(r.rating) AS avg_category_rating
FROM flattened_categories c
LEFT JOIN flattened_products p ON c.category_id = p.category_id
LEFT JOIN flattened_orders o ON p.product_id = o.product_id
LEFT JOIN flattened_reviews r ON p.product_id = r.product_id
GROUP BY c.category_id, c.category_name
ORDER BY total_revenue DESC;

-- ============================================
-- CREATE MATERIALIZED TABLES FOR PERFORMANCE
-- ============================================
CREATE OR REPLACE TABLE dim_products AS SELECT * FROM flattened_products;
CREATE OR REPLACE TABLE dim_categories AS SELECT * FROM flattened_categories;
CREATE OR REPLACE TABLE dim_users AS SELECT DISTINCT * FROM flattened_users;
CREATE OR REPLACE TABLE fact_orders AS SELECT * FROM flattened_orders;
CREATE OR REPLACE TABLE fact_reviews AS SELECT * FROM flattened_reviews;
CREATE OR REPLACE TABLE fact_inventory AS SELECT * FROM flattened_inventory;

-- Add clustering keys for better performance
ALTER TABLE dim_products CLUSTER BY (product_id, category_id);
ALTER TABLE fact_orders CLUSTER BY (order_date, product_id);
ALTER TABLE fact_reviews CLUSTER BY (product_id, rating);