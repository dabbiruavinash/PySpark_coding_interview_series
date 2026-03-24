Read data from one container and procesed based on file types(csv, parquet) and do incremental and also deduplications and also d schema validation and update into log table for reporting to stakeholder and finally load in delta format to proccessed container with schema evalutions

- types (csv, parquet)
- schema validation
- do incremental
- deduplications
- proccessed container with schema evalutions

# data_ingestion_pipeline.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging
from datetime import datetime
import os
from delta.tables import *

class DataIngestionPipeline:
    def __init__(self, config_path='config.yaml'):
        """Initialize Spark session and load configuration"""
        self.spark = SparkSession.builder \
            .appName("DataIngestionPipeline") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
            .enableHiveSupport() \
            .getOrCreate()
        
        # Set log level
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Load configuration
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        
        # Setup logging
        self.setup_logging()
        
        # Initialize tracking variables
        self.processing_stats = {
            'start_time': datetime.now(),
            'files_processed': 0,
            'records_read': 0,
            'records_written': 0,
            'errors': []
        }
    
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f"logs/pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def read_source_data(self, file_path, file_type):
        """Read data based on file type"""
        try:
            if file_type.lower() == 'csv':
                return self.spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .option("multiLine", "true") \
                    .csv(file_path)
            
            elif file_type.lower() == 'parquet':
                return self.spark.read.parquet(file_path)
            
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
        
        except Exception as e:
            self.logger.error(f"Error reading file {file_path}: {str(e)}")
            raise
    
    def validate_schema(self, df, expected_schema, file_name):
        """Validate dataframe schema against expected schema"""
        try:
            validation_results = {
                'is_valid': True,
                'errors': [],
                'warnings': []
            }
            
            # Get current schema
            current_schema = df.schema
            
            # Check for missing columns
            missing_columns = set(expected_schema.fieldNames()) - set(current_schema.fieldNames())
            if missing_columns:
                validation_results['is_valid'] = False
                validation_results['errors'].append(f"Missing columns: {missing_columns}")
            
            # Check for extra columns
            extra_columns = set(current_schema.fieldNames()) - set(expected_schema.fieldNames())
            if extra_columns:
                validation_results['warnings'].append(f"Extra columns found: {extra_columns}")
            
            # Validate data types
            for field in expected_schema.fields:
                if field.name in current_schema.fieldNames():
                    current_field = current_schema[field.name]
                    if current_field.dataType != field.dataType:
                        validation_results['warnings'].append(
                            f"Data type mismatch for column {field.name}: "
                            f"expected {field.dataType}, got {current_field.dataType}"
                        )
            
            # Log validation results
            self.log_validation(file_name, validation_results)
            
            return validation_results
        
        except Exception as e:
            self.logger.error(f"Schema validation error for {file_name}: {str(e)}")
            raise
    
    def perform_incremental_load(self, df, table_name, unique_key):
        """Perform incremental load by identifying new and updated records"""
        try:
            delta_table = DeltaTable.forName(self.spark, table_name)
            
            # Get max timestamp from existing table
            if 'last_modified' in df.columns:
                last_processed = self.get_last_processed_timestamp(table_name)
                if last_processed:
                    df = df.filter(col('last_modified') > last_processed)
            
            # Merge (upsert) operation
            merge_condition = " AND ".join([f"source.{key} = target.{key}" for key in unique_key])
            
            delta_table.alias("target") \
                .merge(
                    df.alias("source"),
                    merge_condition
                ) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
            
            records_affected = df.count()
            self.logger.info(f"Incremental load completed for {table_name}: {records_affected} records upserted")
            
            return records_affected
        
        except Exception as e:
            self.logger.error(f"Incremental load error for {table_name}: {str(e)}")
            raise
    
    def deduplicate_data(self, df, unique_keys, order_by_col=None):
        """Remove duplicates based on unique keys and optional ordering"""
        try:
            if order_by_col:
                # Keep latest record based on order_by column
                window_spec = Window.partitionBy(*unique_keys).orderBy(col(order_by_col).desc())
                df_deduplicated = df.withColumn("row_num", row_number().over(window_spec)) \
                                   .filter(col("row_num") == 1) \
                                   .drop("row_num")
            else:
                # Simple deduplication
                df_deduplicated = df.dropDuplicates(unique_keys)
            
            duplicates_removed = df.count() - df_deduplicated.count()
            self.logger.info(f"Deduplication complete: {duplicates_removed} duplicates removed")
            
            return df_deduplicated
        
        except Exception as e:
            self.logger.error(f"Deduplication error: {str(e)}")
            raise
    
    def log_processing_details(self, file_name, records_read, records_written, 
                               validation_status, error_message=None):
        """Log processing details to tracking table"""
        try:
            log_df = self.spark.createDataFrame([{
                'file_name': file_name,
                'processing_date': datetime.now(),
                'records_read': records_read,
                'records_written': records_written,
                'validation_status': validation_status,
                'error_message': error_message,
                'pipeline_run_id': self.processing_stats.get('run_id', '')
            }])
            
            # Append to log table
            log_df.write \
                .mode("append") \
                .format("delta") \
                .saveAsTable("processed.log_table")
            
            self.logger.info(f"Logged processing details for {file_name}")
        
        except Exception as e:
            self.logger.error(f"Error logging processing details: {str(e)}")
    
    def load_to_processed_container(self, df, table_name, schema_evolution_type='merge'):
        """Load data to processed container with schema evolution"""
        try:
            # Check if table exists
            if self.spark.catalog.tableExists(f"processed.{table_name}"):
                delta_table = DeltaTable.forName(self.spark, f"processed.{table_name}")
                
                # Handle schema evolution
                if schema_evolution_type == 'merge':
                    # Get current schema
                    current_schema = set(delta_table.toDF().schema.fieldNames())
                    new_schema = set(df.schema.fieldNames())
                    
                    # Check for new columns
                    new_columns = new_schema - current_schema
                    if new_columns:
                        self.logger.info(f"New columns detected: {new_columns}")
                        # Write with schema merge option
                        df.write \
                            .mode("append") \
                            .format("delta") \
                            .option("mergeSchema", "true") \
                            .saveAsTable(f"processed.{table_name}")
                    else:
                        # Append data normally
                        df.write \
                            .mode("append") \
                            .format("delta") \
                            .saveAsTable(f"processed.{table_name}")
                else:
                    # Overwrite with schema evolution
                    df.write \
                        .mode("overwrite") \
                        .format("delta") \
                        .option("overwriteSchema", "true") \
                        .saveAsTable(f"processed.{table_name}")
            else:
                # Create new table
                df.write \
                    .mode("overwrite") \
                    .format("delta") \
                    .saveAsTable(f"processed.{table_name}")
            
            self.logger.info(f"Successfully loaded data to processed.{table_name}")
            return df.count()
        
        except Exception as e:
            self.logger.error(f"Error loading to processed container: {str(e)}")
            raise
    
    def process_file(self, file_info):
        """Process single file through the entire pipeline"""
        try:
            file_path = file_info['path']
            file_type = file_info['type']
            table_name = file_info['table_name']
            unique_keys = file_info.get('unique_keys', ['id'])
            expected_schema = self.load_schema(file_info.get('schema_file'))
            incremental_key = file_info.get('incremental_key')
            file_name = os.path.basename(file_path)
            
            self.logger.info(f"Processing file: {file_name}")
            
            # Step 1: Read data
            df = self.read_source_data(file_path, file_type)
            records_read = df.count()
            self.logger.info(f"Read {records_read} records from {file_name}")
            
            # Step 2: Schema validation
            validation_results = self.validate_schema(df, expected_schema, file_name)
            
            if not validation_results['is_valid']:
                raise ValueError(f"Schema validation failed: {validation_results['errors']}")
            
            # Step 3: Deduplication
            df = self.deduplicate_data(df, unique_keys, incremental_key)
            records_after_dedup = df.count()
            
            # Step 4: Incremental load (if applicable)
            if incremental_key and self.spark.catalog.tableExists(f"processed.{table_name}"):
                records_written = self.perform_incremental_load(df, table_name, unique_keys)
            else:
                # Full load
                records_written = self.load_to_processed_container(df, table_name)
            
            # Step 5: Log processing details
            self.log_processing_details(
                file_name=file_name,
                records_read=records_read,
                records_written=records_written,
                validation_status='SUCCESS',
                error_message=None
            )
            
            # Update statistics
            self.processing_stats['files_processed'] += 1
            self.processing_stats['records_read'] += records_read
            self.processing_stats['records_written'] += records_written
            
            self.logger.info(f"Successfully processed {file_name}")
            return True
        
        except Exception as e:
            error_msg = str(e)
            self.logger.error(f"Error processing {file_info.get('path')}: {error_msg}")
            
            # Log failure
            self.log_processing_details(
                file_name=os.path.basename(file_info.get('path', 'unknown')),
                records_read=0,
                records_written=0,
                validation_status='FAILED',
                error_message=error_msg
            )
            
            self.processing_stats['errors'].append({
                'file': file_info.get('path'),
                'error': error_msg
            })
            return False
    
    def load_schema(self, schema_file):
        """Load schema definition from file"""
        if not schema_file:
            return StructType([])  # Return empty schema if not provided
        
        # Load schema from JSON or YAML
        with open(schema_file, 'r') as f:
            schema_config = yaml.safe_load(f)
        
        # Convert to StructType
        fields = []
        for field in schema_config.get('fields', []):
            fields.append(StructField(
                field['name'],
                self.get_spark_type(field['type']),
                field.get('nullable', True)
            ))
        
        return StructType(fields)
    
    def get_spark_type(self, type_str):
        """Convert string type to Spark data type"""
        type_mapping = {
            'string': StringType(),
            'integer': IntegerType(),
            'long': LongType(),
            'double': DoubleType(),
            'float': FloatType(),
            'boolean': BooleanType(),
            'date': DateType(),
            'timestamp': TimestampType(),
            'decimal': DecimalType(38, 18)
        }
        return type_mapping.get(type_str.lower(), StringType())
    
    def get_last_processed_timestamp(self, table_name):
        """Get last processed timestamp from table"""
        try:
            if self.spark.catalog.tableExists(f"processed.{table_name}"):
                df = self.spark.sql(f"SELECT MAX(last_modified) as max_ts FROM processed.{table_name}")
                result = df.collect()[0]['max_ts']
                return result if result else None
        except Exception as e:
            self.logger.warning(f"Could not get last processed timestamp: {str(e)}")
        return None
    
    def generate_report(self):
        """Generate processing report for stakeholders"""
        end_time = datetime.now()
        duration = (end_time - self.processing_stats['start_time']).total_seconds()
        
        report = f"""
        ========================================
        DATA PROCESSING PIPELINE REPORT
        ========================================
        Run ID: {self.processing_stats.get('run_id', 'N/A')}
        Start Time: {self.processing_stats['start_time']}
        End Time: {end_time}
        Duration: {duration:.2f} seconds
        
        Processing Statistics:
        - Files Processed: {self.processing_stats['files_processed']}
        - Total Records Read: {self.processing_stats['records_read']}
        - Total Records Written: {self.processing_stats['records_written']}
        - Errors Encountered: {len(self.processing_stats['errors'])}
        
        Error Details:
        {self.processing_stats['errors'] if self.processing_stats['errors'] else 'No errors'}
        ========================================
        """
        
        # Save report
        report_path = f"reports/pipeline_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        os.makedirs("reports", exist_ok=True)
        
        with open(report_path, 'w') as f:
            f.write(report)
        
        self.logger.info(f"Report generated: {report_path}")
        print(report)
        
        return report
    
    def run_pipeline(self):
        """Main pipeline execution"""
        try:
            self.processing_stats['run_id'] = datetime.now().strftime('%Y%m%d_%H%M%S')
            self.logger.info(f"Starting pipeline run: {self.processing_stats['run_id']}")
            
            # Process each file from configuration
            for file_info in self.config.get('files', []):
                self.process_file(file_info)
            
            # Generate final report
            self.generate_report()
            
            self.logger.info("Pipeline completed successfully")
            return True
        
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            raise
        finally:
            self.spark.stop()

if __name__ == "__main__":
    pipeline = DataIngestionPipeline('config.yaml')
    pipeline.run_pipeline()