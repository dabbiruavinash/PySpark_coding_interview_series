from pyspark.sql.functions import broadcast
    
    dept_avg = employees_df.groupBy("department_id") \
        .agg(avg("salary").alias("dept_avg_salary"))
    
    above_avg_alt = employees_df.join(
        broadcast(dept_avg), 
        "department_id").filter(col("salary") > col("dept_avg_salary"))
    
    return above_avg_employees

%sql
SELECT 
    employee_id,
    employee_name,
    department_id,
    salary,
    AVG(salary) OVER (PARTITION BY department_id) AS dept_avg_salary
FROM employees
WHERE salary > AVG(salary) OVER (PARTITION BY department_id);