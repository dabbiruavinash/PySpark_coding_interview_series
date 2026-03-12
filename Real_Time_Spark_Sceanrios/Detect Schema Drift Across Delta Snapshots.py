from delta.tables import DeltaTable
from pyspark.sql.functions import col, struct, to_json, from_json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

def detect_schema_drift(spark, table_path, num_versions=10):
    delta_table = DeltaTable.forPath(spark, table_path)
    
    # Get historical versions
    history_df = delta_table.history(num_versions) \
        .select("version", "timestamp", "operation", "operationParameters")
    
    # Extract schema for each version
    schema_evolution = []
    
    for version in range(num_versions):
        try:
            # Time travel to specific version
            df = spark.read.format("delta") \
                .option("versionAsOf", version) \
                .load(table_path)
            
            current_schema = df.schema
            schema_evolution.append({
                "version": version,
                "fields": [f.name for f in current_schema.fields],
                "field_count": len(current_schema.fields),
                "field_types": {f.name: str(f.dataType) for f in current_schema.fields}
            })
        except:
            print(f"Version {version} not available")
    
    # Detect changes between versions
    drift_detection = []
    for i in range(1, len(schema_evolution)):
        prev = schema_evolution[i-1]
        curr = schema_evolution[i]
        
        added_fields = set(curr['fields']) - set(prev['fields'])
        removed_fields = set(prev['fields']) - set(curr['fields'])
        type_changes = {}
        
        for field in set(prev['fields']) & set(curr['fields']):
            if prev['field_types'][field] != curr['field_types'][field]:
                type_changes[field] = {
                    "from": prev['field_types'][field],
                    "to": curr['field_types'][field]
                }
        
        if added_fields or removed_fields or type_changes:
            drift_detection.append({
                "from_version": prev['version'],
                "to_version": curr['version'],
                "added_fields": list(added_fields),
                "removed_fields": list(removed_fields),
                "type_changes": type_changes
            })
    
    # Remediation steps
    def remediate_schema_drift(spark, table_path, strategy="merge"):
        if strategy == "merge":
            # Read with mergeSchema option
            df = spark.read.format("delta") \
                .option("mergeSchema", "true") \
                .load(table_path)
            
            # Write back with unified schema
            df.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .save(table_path)
        
        elif strategy == "evolve":
            # Use schema evolution in merge
            from delta.tables import DeltaTable
            
            delta_table = DeltaTable.forPath(spark, table_path)
            
            # Example merge with schema evolution
            new_data_df = spark.createDataFrame([], StructType([]))  # Your new data
            
            delta_table.alias("target") \
                .merge(
                    new_data_df.alias("source"),
                    "target.id = source.id"
                ) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
    
    return drift_detection

# Auto-remediation function
def auto_remediate_schema(spark, table_path):
    drift = detect_schema_drift(spark, table_path)
    
    if drift:
        print(f"Detected {len(drift)} schema drifts")
        # Create unified schema from all versions
        all_fields = {}
        
        for version in range(10):
            df = spark.read.format("delta") \
                .option("versionAsOf", version) \
                .load(table_path)
            
            for field in df.schema.fields:
                if field.name not in all_fields:
                    all_fields[field.name] = field.dataType
        
        # Recreate table with unified schema
        unified_schema = StructType([
            StructField(name, dtype, True) 
            for name, dtype in all_fields.items()
        ])
        
        # Read and union all versions with unified schema
        unified_df = None
        for version in range(10):
            df = spark.read.format("delta") \
                .option("versionAsOf", version) \
                .load(table_path)
            
            # Cast to unified schema
            for field in unified_schema.fields:
                if field.name not in df.columns:
                    df = df.withColumn(field.name, lit(None).cast(field.dataType))
            
            if unified_df is None:
                unified_df = df
            else:
                unified_df = unified_df.unionByName(df, allowMissingColumns=True)
        
        # Write back
        unified_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(table_path)
        
        return True
    
    return False

%sql
-- Check for schema changes in Oracle (using data dictionary)
-- Detect new columns added
SELECT 
    table_name,
    column_name,
    data_type,
    data_length,
    nullable,
    last_analyzed
FROM user_tab_columns
WHERE table_name = 'YOUR_TABLE'
ORDER BY column_id;

-- Compare with historical snapshot (if using Flashback)
SELECT 
    column_name,
    data_type,
    data_length
FROM your_table AS OF TIMESTAMP (SYSTIMESTAMP - INTERVAL '30' DAY)
WHERE ROWNUM = 0;  -- Just get schema

-- Track DDL changes
SELECT 
    timestamp,
    operation,
    search_object_name,
    sql_redo
FROM dba_hist_sqltext
WHERE UPPER(sql_text) LIKE '%ALTER TABLE YOUR_TABLE%'
ORDER BY timestamp DESC;

-- Remediation: Add missing columns
DECLARE
    CURSOR missing_columns IS
        SELECT column_name, data_type, data_length
        FROM expected_schema
        MINUS
        SELECT column_name, data_type, data_length
        FROM user_tab_columns
        WHERE table_name = 'YOUR_TABLE';
BEGIN
    FOR col IN missing_columns LOOP
        EXECUTE IMMEDIATE 'ALTER TABLE your_table ADD (' || 
                         col.column_name || ' ' || col.data_type || 
                         ')';
    END LOOP;
END;
/