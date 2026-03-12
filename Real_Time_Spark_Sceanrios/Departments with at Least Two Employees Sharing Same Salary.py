from pyspark.sql import Window
from pyspark.sql.functions import col, count, dense_rank, row_number

def departments_with_duplicate_salaries(employees_df):
    # Method 1: Group by department and salary
    salary_groups = employees_df.groupBy("department_id", "salary") \
        .agg(count("*").alias("employee_count")) \
        .filter(col("employee_count") >= 2) \
        .select("department_id").distinct()

%sql
SELECT DISTINCT department_id FROM employees GROUP BY department_id, salary HAVING COUNT(*) >= 2;