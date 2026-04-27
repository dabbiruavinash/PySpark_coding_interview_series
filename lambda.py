import boto3
import json
import csv
import gzip
import io
import hashlib
import logging
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from botocore.exceptions import ClientError, BotoCoreError
import pyarrow.parquet as pq
import pandas as pd

# Initialize clients
s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb')
glue = boto3.client('glue')
athena = boto3.client('athena')

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuration
DEAD_LETTER_BUCKET = 'my-dlq-bucket'
UNPROCESSABLE_BUCKET = 'my-unprocessable-data'
PROCESSED_BUCKET = 'my-processed-data'
METADATA_TABLE = 's3-etl-metadata'
MAX_RECORDS = 100000
MAX_FILE_SIZE_MB = 500
VALID_SCHEMA_VERSION = '2.0.0'

def lambda_handler(event, context):
    """
    Main handler with 10 critical conditions for S3 to S3 data loading
    """
    execution_id = context.aws_request_id
    logger.info(f"Starting ETL execution {execution_id}")
    
    results = {
        'execution_id': execution_id,
        'processed_files': 0,
        'failed_files': 0,
        'records_processed': 0,
        'conditions_triggered': []
    }
    
    # Get source bucket and key from event
    # Supports multiple event sources: S3 trigger, EventBridge, or direct invocation
    source_buckets = extract_source_buckets(event)
    
    for source_bucket, source_keys in source_buckets.items():
        for source_key in source_keys:
            try:
                # CONDITION 1: Validate file size and type before processing
                condition1_status = validate_file_constraints(source_bucket, source_key)
                if not condition1_status['passed']:
                    results['conditions_triggered'].append(condition1_status)
                    move_to_dlq(source_bucket, source_key, condition1_status['reason'])
                    results['failed_files'] += 1
                    continue
                
                # CONDITION 2: Check for duplicate processing (idempotency)
                condition2_status = check_duplicate_processing(source_key, execution_id)
                if not condition2_status['passed']:
                    results['conditions_triggered'].append(condition2_status)
                    logger.warning(f"Skipping duplicate file: {source_key}")
                    continue
                
                # CONDITION 3: Validate data schema and format
                file_content = read_s3_file(source_bucket, source_key)
                condition3_status = validate_schema(file_content, source_key)
                if not condition3_status['passed']:
                    results['conditions_triggered'].append(condition3_status)
                    move_to_unprocessable_bucket(source_bucket, source_key, condition3_status['reason'])
                    results['failed_files'] += 1
                    continue
                
                # CONDITION 4: Data quality checks (nulls, duplicates, outliers)
                transformed_data, condition4_status = data_quality_checks(file_content)
                if not condition4_status['passed']:
                    results['conditions_triggered'].append(condition4_status)
                    # Log but continue with cleaned data
                    logger.warning(f"Data quality issues found: {condition4_status['reason']}")
                
                # CONDITION 5: Apply business transformation rules
                condition5_status, transformed_data = apply_business_rules(transformed_data)
                if not condition5_status['passed']:
                    results['conditions_triggered'].append(condition5_status)
                    move_to_unprocessable_bucket(source_bucket, source_key, condition5_status['reason'])
                    results['failed_files'] += 1
                    continue
                
                # CONDITION 6: Data encryption and PII masking
                condition6_status = apply_pii_masking(transformed_data)
                if not condition6_status['passed']:
                    results['conditions_triggered'].append(condition6_status)
                    # Security violation - move to secure quarantine
                    move_to_secure_quarantine(source_bucket, source_key, condition6_status['reason'])
                    results['failed_files'] += 1
                    continue
                
                # CONDITION 7: Partitioning strategy based on data
                partition_path, condition7_status = determine_partition_strategy(transformed_data)
                if not condition7_status['passed']:
                    results['conditions_triggered'].append(condition7_status)
                    partition_path = 'failed_partition/data'  # fallback
                
                # CONDITION 8: Atomic write with checksum validation
                target_key = f"{partition_path}/{datetime.now().strftime('%Y/%m/%d')}/{source_key.split('/')[-1]}"
                condition8_status = atomic_write_to_s3(transformed_data, PROCESSED_BUCKET, target_key)
                if not condition8_status['passed']:
                    results['conditions_triggered'].append(condition8_status)
                    # Retry logic with exponential backoff
                    retry_status = retry_write(transformed_data, PROCESSED_BUCKET, target_key, 3)
                    if not retry_status['passed']:
                        move_to_dlq(source_bucket, source_key, retry_status['reason'])
                        results['failed_files'] += 1
                        continue
                
                # CONDITION 9: Update metadata catalog (Glue/Athena)
                condition9_status = update_glue_catalog(target_key, partition_path, len(transformed_data))
                if not condition9_status['passed']:
                    results['conditions_triggered'].append(condition9_status)
                    # Non-critical, continue but log error
                    logger.error(f"Failed to update Glue catalog: {condition9_status['reason']}")
                
                # CONDITION 10: Real-time monitoring and alerting
                condition10_status = trigger_monitoring_alert(results, transformed_data)
                if not condition10_status['passed']:
                    results['conditions_triggered'].append(condition10_status)
                    # Send to dead letter for manual review
                    move_to_dlq(source_bucket, source_key, "Monitoring alert threshold exceeded")
                    results['failed_files'] += 1
                    continue
                
                # All conditions passed - move source file to archive
                archive_source_file(source_bucket, source_key, execution_id)
                results['processed_files'] += 1
                results['records_processed'] += len(transformed_data)
                
                # Update success metadata
                update_processing_metadata(source_key, 'SUCCESS', execution_id, len(transformed_data))
                
            except Exception as e:
                logger.error(f"Unexpected error processing {source_key}: {str(e)}")
                results['failed_files'] += 1
                move_to_dlq(source_bucket, source_key, f"Unexpected error: {str(e)}")
                update_processing_metadata(source_key, 'FAILED', execution_id, 0, str(e))
    
    # Final aggregation and reporting
    finalize_execution(results)
    return results

# ============= CONDITION 1 IMPLEMENTATION =============
def validate_file_constraints(bucket, key):
    """Validate file size, type, and basic accessibility"""
    try:
        # Get file metadata
        response = s3.head_object(Bucket=bucket, Key=key)
        file_size_mb = response['ContentLength'] / (1024 * 1024)
        
        # Check file size
        if file_size_mb > MAX_FILE_SIZE_MB:
            return {
                'passed': False,
                'condition': 1,
                'reason': f'File size {file_size_mb:.2f}MB exceeds limit of {MAX_FILE_SIZE_MB}MB',
                'severity': 'HIGH'
            }
        
        # Check file extension
        allowed_extensions = ['.csv', '.json', '.parquet', '.avro']
        if not any(key.endswith(ext) for ext in allowed_extensions):
            return {
                'passed': False,
                'condition': 1,
                'reason': f'Unsupported file type. Allowed: {allowed_extensions}',
                'severity': 'HIGH'
            }
        
        # Check if file is empty
        if response['ContentLength'] == 0:
            return {
                'passed': False,
                'condition': 1,
                'reason': 'File is empty',
                'severity': 'MEDIUM'
            }
        
        # Check last modified timestamp (avoid processing very old files)
        last_modified = response['LastModified']
        days_old = (datetime.now(timezone.utc) - last_modified).days
        if days_old > 30:
            return {
                'passed': False,
                'condition': 1,
                'reason': f'File is {days_old} days old. Stale data not processed',
                'severity': 'LOW'
            }
        
        return {'passed': True, 'condition': 1}
    
    except ClientError as e:
        return {
            'passed': False,
            'condition': 1,
            'reason': f"Cannot access file: {str(e)}",
            'severity': 'CRITICAL'
        }

# ============= CONDITION 2 IMPLEMENTATION =============
def check_duplicate_processing(file_key, execution_id):
    """Check DynamoDB for duplicate processing attempts within SLA window"""
    table = dynamodb.Table(METADATA_TABLE)
    
    try:
        response = table.get_item(Key={'file_key': file_key})
        
        if 'Item' in response:
            last_processed = response['Item']['last_processed_time']
            last_status = response['Item']['status']
            processing_count = response['Item'].get('processing_count', 0)
            
            # Check if processed within last hour
            time_diff = (datetime.now(timezone.utc) - last_processed).total_seconds() / 3600
            
            if time_diff < 1 and last_status == 'SUCCESS':
                return {
                    'passed': False,
                    'condition': 2,
                    'reason': f'File already processed successfully {time_diff:.2f} hours ago',
                    'severity': 'HIGH'
                }
            
            # Max retry limit
            if processing_count >= 3:
                return {
                    'passed': False,
                    'condition': 2,
                    'reason': f'Max retry limit reached ({processing_count} attempts)',
                    'severity': 'HIGH'
                }
            
            # Update processing count
            table.update_item(
                Key={'file_key': file_key},
                UpdateExpression='ADD processing_count :inc SET last_attempt_time = :time, last_attempt_id = :exec_id',
                ExpressionAttributeValues={
                    ':inc': 1,
                    ':time': datetime.now(timezone.utc),
                    ':exec_id': execution_id
                }
            )
        else:
            # First time processing
            table.put_item(Item={
                'file_key': file_key,
                'first_seen_time': datetime.now(timezone.utc),
                'processing_count': 1,
                'status': 'IN_PROGRESS',
                'last_attempt_id': execution_id
            })
        
        return {'passed': True, 'condition': 2}
    
    except Exception as e:
        logger.error(f"DynamoDB check failed: {str(e)}")
        # Allow processing if metadata store is down (fail open)
        return {'passed': True, 'condition': 2, 'warning': 'Metadata check failed'}

# ============= CONDITION 3 IMPLEMENTATION =============
def validate_schema(file_content, file_key):
    """Validate data schema against expected schema version"""
    try:
        # Parse based on file extension
        data = parse_data_by_format(file_content, file_key)
        
        if not data or len(data) == 0:
            return {
                'passed': False,
                'condition': 3,
                'reason': 'No valid data records found',
                'severity': 'HIGH'
            }
        
        # Expected schema definition
        expected_schema = {
            'required_fields': ['id', 'timestamp', 'customer_id', 'amount'],
            'data_types': {
                'id': str,
                'customer_id': str,
                'amount': (float, int, Decimal),
                'timestamp': str,
                'status': str,
                'region': str
            },
            'allowed_values': {
                'status': ['active', 'pending', 'completed', 'cancelled'],
                'region': ['NA', 'EU', 'APAC', 'LATAM']
            }
        }
        
        # Check first record for schema
        first_record = data[0]
        
        # Check required fields
        missing_fields = [f for f in expected_schema['required_fields'] if f not in first_record]
        if missing_fields:
            return {
                'passed': False,
                'condition': 3,
                'reason': f'Missing required fields: {missing_fields}',
                'severity': 'HIGH'
            }
        
        # Check data types
        type_mismatches = []
        for field, expected_type in expected_schema['data_types'].items():
            if field in first_record:
                actual_value = first_record[field]
                if not isinstance(actual_value, expected_type):
                    # Handle Decimal to numeric conversion
                    if isinstance(actual_value, Decimal) and expected_type in (float, int):
                        continue
                    type_mismatches.append(f"{field}: expected {expected_type}, got {type(actual_value)}")
        
        if type_mismatches:
            return {
                'passed': False,
                'condition': 3,
                'reason': f'Data type mismatches: {type_mismatches[:5]}',
                'severity': 'MEDIUM'
            }
        
        # Check schema version compatibility
        file_schema_version = first_record.get('schema_version', '1.0.0')
        if file_schema_version != VALID_SCHEMA_VERSION:
            return {
                'passed': False,
                'condition': 3,
                'reason': f'Schema version mismatch. Expected {VALID_SCHEMA_VERSION}, got {file_schema_version}',
                'severity': 'HIGH'
            }
        
        return {'passed': True, 'condition': 3}
    
    except Exception as e:
        return {
            'passed': False,
            'condition': 3,
            'reason': f'Schema validation error: {str(e)}',
            'severity': 'CRITICAL'
        }

# ============= CONDITION 4 IMPLEMENTATION =============
def data_quality_checks(data):
    """Perform comprehensive data quality checks"""
    issues = []
    cleaned_data = []
    
    total_records = len(data)
    if total_records == 0:
        return [], {'passed': False, 'condition': 4, 'reason': 'No data to process'}
    
    # Check 1: Null value percentage
    null_counts = {}
    for record in data:
        for field, value in record.items():
            if value is None or value == '' or (isinstance(value, str) and value.strip() == ''):
                null_counts[field] = null_counts.get(field, 0) + 1
    
    for field, null_count in null_counts.items():
        null_percentage = (null_count / total_records) * 100
        if null_percentage > 30:
            issues.append(f"Field '{field}' has {null_percentage:.1f}% null values (threshold: 30%)")
    
    # Check 2: Duplicate detection
    seen_ids = set()
    duplicates = []
    for record in data:
        record_id = record.get('id')
        if record_id:
            if record_id in seen_ids:
                duplicates.append(record_id)
            else:
                seen_ids.add(record_id)
    
    if len(duplicates) > 0:
        issues.append(f"Found {len(duplicates)} duplicate record IDs")
    
    # Check 3: Outlier detection (numerical fields)
    if 'amount' in data[0]:
        amounts = [Decimal(str(r.get('amount', 0))) for r in data if r.get('amount')]
        if amounts:
            mean_amount = sum(amounts) / len(amounts)
            std_amount = (sum((a - mean_amount) ** 2 for a in amounts) / len(amounts)) ** 0.5
            outliers = [a for a in amounts if abs(a - mean_amount) > 3 * std_amount]
            if len(outliers) > total_records * 0.05:  # >5% outliers
                issues.append(f"Found {len(outliers)} outliers in 'amount' field (>3 std dev)")
    
    # Check 4: Date format validation
    date_pattern = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}')
    invalid_dates = []
    for record in data:
        timestamp = record.get('timestamp', '')
        if timestamp and not date_pattern.match(timestamp):
            invalid_dates.append(timestamp)
    
    if len(invalid_dates) > total_records * 0.1:
        issues.append(f"{len(invalid_dates)} records have invalid timestamp format")
    
    # Clean data based on issues
    for record in data:
        # Remove records with critical nulls
        if not record.get('id') or not record.get('customer_id'):
            continue  # Skip records missing critical fields
        
        # Fill default values for non-critical nulls
        if not record.get('status'):
            record['status'] = 'unknown'
        if not record.get('region'):
            record['region'] = 'UNKNOWN'
        
        # Fix data types
        if 'amount' in record and record['amount'] is not None:
            try:
                record['amount'] = float(record['amount'])
            except (ValueError, TypeError):
                record['amount'] = 0.0
        
        cleaned_data.append(record)
    
    passed = len(issues) <= 3  # Allow up to 3 quality issues
    return cleaned_data, {
        'passed': passed,
        'condition': 4,
        'reason': '; '.join(issues) if issues else 'All quality checks passed',
        'severity': 'MEDIUM' if issues else 'LOW'
    }

# ============= CONDITION 5 IMPLEMENTATION =============
def apply_business_rules(data):
    """Apply complex business transformation rules"""
    transformed_data = []
    transformation_log = []
    
    for record in data:
        try:
            # Rule 1: Currency conversion based on region
            region = record.get('region', 'NA')
            amount = record.get('amount', 0)
            
            exchange_rates = {
                'EU': 0.92,  # USD to EUR
                'APAC': 110.5,  # USD to JPY
                'LATAM': 20.5,  # USD to MXN
                'NA': 1.0
            }
            
            record['amount_local'] = amount * exchange_rates.get(region, 1.0)
            record['currency'] = {
                'EU': 'EUR',
                'APAC': 'JPY',
                'LATAM': 'MXN',
                'NA': 'USD'
            }.get(region, 'USD')
            
            # Rule 2: Calculate tax based on transaction type
            status = record.get('status', 'pending')
            tax_rates = {
                'active': 0.10,
                'completed': 0.08,
                'pending': 0.05,
                'cancelled': 0.0
            }
            record['tax_amount'] = record['amount_local'] * tax_rates.get(status, 0.05)
            record['total_amount'] = record['amount_local'] + record['tax_amount']
            
            # Rule 3: Add processing timestamp and partition keys
            record['processed_timestamp'] = datetime.now(timezone.utc).isoformat()
            record['year'] = datetime.now().year
            record['month'] = datetime.now().month
            record['day'] = datetime.now().day
            
            # Rule 4: Data enrichment from external source (if needed)
            if record.get('customer_id'):
                record['customer_segment'] = get_customer_segment(record['customer_id'])
            
            # Rule 5: Flag suspicious transactions
            if record['total_amount'] > 10000:
                record['suspicious_flag'] = True
                record['suspicious_reason'] = 'High value transaction'
                transformation_log.append(f"Suspicious transaction: {record['id']}")
            elif record.get('region') == 'APAC' and record.get('status') == 'active':
                record['suspicious_flag'] = True
                record['suspicious_reason'] = 'Unusual pattern for region'
            else:
                record['suspicious_flag'] = False
            
            transformed_data.append(record)
            
        except Exception as e:
            transformation_log.append(f"Failed to transform record {record.get('id')}: {str(e)}")
            continue
    
    if len(transformed_data) == 0:
        return {
            'passed': False,
            'condition': 5,
            'reason': 'No records passed business rule transformation',
            'severity': 'HIGH'
        }, []
    
    if len(transformed_data) < len(data) * 0.8:
        return {
            'passed': False,
            'condition': 5,
            'reason': f'High failure rate: {len(transformed_data)}/{len(data)} records transformed',
            'severity': 'HIGH'
        }, transformed_data
    
    return {'passed': True, 'condition': 5}, transformed_data

def get_customer_segment(customer_id):
    """Mock function to demonstrate external enrichment"""
    # In production, this would call a database or API
    hash_val = hash(customer_id) % 5
    segments = ['Tier1', 'Tier2', 'Tier3', 'Enterprise', 'Startup']
    return segments[hash_val]

# ============= CONDITION 6 IMPLEMENTATION =============
def apply_pii_masking(data):
    """Mask or encrypt PII (Personally Identifiable Information)"""
    import re
    
    # Define PII patterns
    pii_patterns = {
        'email': re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'),
        'phone': re.compile(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b'),
        'ssn': re.compile(r'\b\d{3}-\d{2}-\d{4}\b'),
        'credit_card': re.compile(r'\b(?:\d{4}[-\s]?){3}\d{4}\b')
    }
    
    masked_data = []
    violations = []
    
    for record in data:
        # Check for unencrypted PII fields
        for field, value in record.items():
            if isinstance(value, str):
                # Auto-detect PII patterns
                for pii_type, pattern in pii_patterns.items():
                    if pattern.search(value):
                        if field not in ['email', 'phone']:  # Allow in specific fields
                            violations.append(f"Field '{field}' contains {pii_type}")
                        else:
                            # Mask the value
                            if pii_type == 'email':
                                parts = value.split('@')
                                record[field] = f"{parts[0][:3]}***@{parts[1]}"
                            elif pii_type == 'phone':
                                record[field] = f"***-***-{value[-4:]}"
                            elif pii_type == 'ssn':
                                record[field] = f"***-**-{value[-4:]}"
                            elif pii_type == 'credit_card':
                                record[field] = f"XXXX-XXXX-XXXX-{value[-4:]}"
        
        # Check for specific PII fields that should be encrypted
        sensitive_fields = ['customer_email', 'customer_phone', 'billing_address']
        for field in sensitive_fields:
            if field in record and record[field]:
                # In production, use KMS encryption
                record[f'{field}_encrypted'] = encrypt_data(str(record[field]))
                del record[field]  # Remove plaintext PII
        
        masked_data.append(record)
    
    if len(violations) > len(data) * 0.1:  # >10% of records have PII violations
        return {
            'passed': False,
            'condition': 6,
            'reason': f'PII compliance violation: {violations[:5]}',
            'severity': 'CRITICAL'
        }
    
    return {'passed': True, 'condition': 6}

def encrypt_data(plaintext):
    """Placeholder for KMS encryption"""
    # In production, use: kms.encrypt(KeyId=key_id, Plaintext=plaintext)
    return f"ENC::{hashlib.sha256(plaintext.encode()).hexdigest()[:32]}"

# ============= CONDITION 7 IMPLEMENTATION =============
def determine_partition_strategy(data):
    """Intelligent partitioning based on data characteristics"""
    if not data or len(data) == 0:
        return 'unpartitioned', {'passed': False, 'condition': 7, 'reason': 'No data to partition'}
    
    # Analyze data to determine optimal partition
    partition_candidates = {}
    
    # Candidate 1: Date-based partitioning (if timestamp field exists)
    if 'timestamp' in data[0]:
        unique_dates = set()
        for record in data[:100]:  # Sample first 100 records
            try:
                date_part = record['timestamp'][:10] if record.get('timestamp') else 'unknown'
                unique_dates.add(date_part)
            except:
                pass
        
        if len(unique_dates) <= 10:  # Good cardinality for partitioning
            partition_candidates['date'] = len(unique_dates)
    
    # Candidate 2: Region partitioning
    if 'region' in data[0]:
        unique_regions = set(r.get('region') for r in data if r.get('region'))
        if 2 <= len(unique_regions) <= 10:
            partition_candidates['region'] = len(unique_regions)
    
    # Candidate 3: Status partitioning
    if 'status' in data[0]:
        unique_status = set(r.get('status') for r in data if r.get('status'))
        if 2 <= len(unique_status) <= 10:
            partition_candidates['status'] = len(unique_status)
    
    # Choose best partition (lowest cardinality for even distribution)
    if partition_candidates:
        best_partition = min(partition_candidates, key=partition_candidates.get)
        
        if best_partition == 'date':
            date_val = data[0].get('timestamp', '2024-01-01')[:10]
            partition_path = f"dt={date_val}"
        elif best_partition == 'region':
            region_val = data[0].get('region', 'unknown')
            partition_path = f"region={region_val}"
        elif best_partition == 'status':
            status_val = data[0].get('status', 'unknown')
            partition_path = f"status={status_val}"
        else:
            partition_path = 'default_partition'
        
        return partition_path, {'passed': True, 'condition': 7}
    else:
        return 'default_partition', {'passed': True, 'condition': 7, 'warning': 'Using default partition'}

# ============= CONDITION 8 IMPLEMENTATION =============
def atomic_write_to_s3(data, bucket, key):
    """Atomic write with checksum verification"""
    try:
        # Convert data to appropriate format (determine by key extension)
        if key.endswith('.json'):
            output = json.dumps(data, default=str).encode('utf-8')
            content_type = 'application/json'
        elif key.endswith('.parquet'):
            # Convert to Parquet for better performance
            df = pd.DataFrame(data)
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            output = buffer.getvalue()
            content_type = 'application/parquet'
        else:
            output = json.dumps(data, default=str).encode('utf-8')
            content_type = 'application/json'
        
        # Generate checksum
        checksum = hashlib.md5(output).hexdigest()
        
        # Write to temporary location first
        temp_key = f"temp/{key}.{context.aws_request_id}"
        s3.put_object(
            Bucket=bucket,
            Key=temp_key,
            Body=output,
            ContentType=content_type,
            Metadata={
                'checksum': checksum,
                'record_count': str(len(data)),
                'created_at': datetime.now(timezone.utc).isoformat()
            }
        )
        
        # Verify temp file exists and checksum matches
        response = s3.head_object(Bucket=bucket, Key=temp_key)
        if response['Metadata'].get('checksum') != checksum:
            raise ValueError("Checksum verification failed")
        
        # Atomic move (copy then delete)
        s3.copy_object(
            CopySource={'Bucket': bucket, 'Key': temp_key},
            Bucket=bucket,
            Key=key,
            MetadataDirective='COPY'
        )
        
        # Clean up temp file
        s3.delete_object(Bucket=bucket, Key=temp_key)
        
        return {'passed': True, 'condition': 8}
        
    except Exception as e:
        return {
            'passed': False,
            'condition': 8,
            'reason': f'Atomic write failed: {str(e)}',
            'severity': 'HIGH'
        }

def retry_write(data, bucket, key, max_retries):
    """Exponential backoff retry for failed writes"""
    import time
    
    for attempt in range(max_retries):
        result = atomic_write_to_s3(data, bucket, key)
        if result['passed']:
            return result
        
        wait_time = 2 ** attempt  # Exponential backoff
        logger.warning(f"Write attempt {attempt + 1} failed, retrying in {wait_time}s")
        time.sleep(wait_time)
    
    return {
        'passed': False,
        'condition': 8,
        'reason': f'Failed after {max_retries} retries',
        'severity': 'CRITICAL'
    }

# ============= CONDITION 9 IMPLEMENTATION =============
def update_glue_catalog(s3_key, partition_path, record_count):
    """Update AWS Glue Data Catalog for Athena querying"""
    try:
        database_name = 'my_analytics_db'
        table_name = 'processed_transactions'
        
        # Extract partition values from path
        # Example: dt=2024-01-15/region=NA/file.json
        partition_values = {}
        path_parts = s3_key.split('/')
        for part in path_parts:
            if '=' in part:
                key, value = part.split('=')
                partition_values[key] = value
        
        if partition_values:
            # Add partition to Glue catalog
            try:
                glue.create_partition(
                    DatabaseName=database_name,
                    TableName=table_name,
                    PartitionInput={
                        'Values': list(partition_values.values()),
                        'StorageDescriptor': {
                            'Location': f's3://{PROCESSED_BUCKET}/{"/".join(path_parts[:-1])}',
                            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                            'SerdeInfo': {
                                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                            },
                            'Columns': [
                                {'Name': 'id', 'Type': 'string'},
                                {'Name': 'customer_id', 'Type': 'string'},
                                {'Name': 'amount', 'Type': 'double'},
                                {'Name': 'timestamp', 'Type': 'timestamp'}
                            ]
                        }
                    }
                )
                logger.info(f"Added partition {partition_values} to Glue catalog")
            except glue.exceptions.AlreadyExistsException:
                # Partition already exists, update statistics
                glue.update_partition(
                    DatabaseName=database_name,
                    TableName=table_name,
                    PartitionValueList=list(partition_values.values()),
                    PartitionInput={
                        'StorageDescriptor': {
                            'Location': f's3://{PROCESSED_BUCKET}/{"/".join(path_parts[:-1])}'
                        },
                        'Parameters': {
                            'record_count': str(record_count),
                            'last_updated': datetime.now(timezone.utc).isoformat()
                        }
                    }
                )
        
        # Refresh partition metadata for Athena
        athena.start_query_execution(
            QueryString=f"MSCK REPAIR TABLE {table_name}",
            QueryExecutionContext={'Database': database_name},
            ResultConfiguration={'OutputLocation': f's3://{PROCESSED_BUCKET}/athena-results/'}
        )
        
        return {'passed': True, 'condition': 9}
        
    except Exception as e:
        return {
            'passed': False,
            'condition': 9,
            'reason': f'Glue catalog update failed: {str(e)}',
            'severity': 'MEDIUM'
        }

# ============= CONDITION 10 IMPLEMENTATION =============
def trigger_monitoring_alert(results, data):
    """Real-time monitoring with threshold-based alerting"""
    import urllib3
    
    # Define thresholds
    thresholds = {
        'max_failure_rate': 0.1,  # 10% failure rate
        'min_records_per_file': 1,
        'max_latency_seconds': 300,
        'critical_condition_severity': 'CRITICAL'
    }
    
    alerts = []
    
    # Check 1: Failure rate
    total_files = results['processed_files'] + results['failed_files']
    if total_files > 0:
        failure_rate = results['failed_files'] / total_files
        if failure_rate > thresholds['max_failure_rate']:
            alerts.append({
                'type': 'HIGH_FAILURE_RATE',
                'message': f'Failure rate {failure_rate:.1%} exceeds threshold',
                'severity': 'HIGH'
            })
    
    # Check 2: Data volume anomaly
    if len(data) < thresholds['min_records_per_file']:
        alerts.append({
            'type': 'LOW_DATA_VOLUME',
            'message': f'Only {len(data)} records processed',
            'severity': 'MEDIUM'
        })
    
    # Check 3: Detect suspicious patterns in data
    suspicious_count = sum(1 for r in data if r.get('suspicious_flag', False))
    if suspicious_count > len(data) * 0.05:  # >5% suspicious
        alerts.append({
            'type': 'HIGH_SUSPICIOUS_ACTIVITY',
            'message': f'{suspicious_count} suspicious transactions detected',
            'severity': 'HIGH'
        })
    
    # Check 4: Alert on critical conditions
    critical_conditions = [c for c in results['conditions_triggered'] 
                          if c.get('severity') == 'CRITICAL']
    if critical_conditions:
        alerts.append({
            'type': 'CRITICAL_CONDITIONS',
            'message': f'{len(critical_conditions)} critical conditions triggered',
            'details': critical_conditions,
            'severity': 'CRITICAL'
        })
    
    # Send alerts to monitoring system
    if alerts:
        http = urllib3.PoolManager()
        
        for alert in alerts:
            # Send to CloudWatch
            put_metric_data(alert)
            
            # Send to SNS for email/SMS
            send_sns_alert(alert)
            
            # Send to Slack webhook (for real-time notifications)
            if alert['severity'] in ['HIGH', 'CRITICAL']:
                slack_webhook = os.environ.get('SLACK_WEBHOOK_URL')
                if slack_webhook:
                    http.request(
                        'POST',
                        slack_webhook,
                        body=json.dumps({
                            'text': f"[{alert['severity']}] ETL Alert: {alert['message']}",
                            'attachments': [{
                                'color': 'danger' if alert['severity'] == 'CRITICAL' else 'warning',
                                'fields': [
                                    {'title': 'Execution ID', 'value': results['execution_id'], 'short': True},
                                    {'title': 'Processed', 'value': results['processed_files'], 'short': True},
                                    {'title': 'Failed', 'value': results['failed_files'], 'short': True}
                                ]
                            }]
                        }).encode('utf-8'),
                        headers={'Content-Type': 'application/json'}
                    )
        
        # If critical alerts exist, fail the execution
        if any(alert['severity'] == 'CRITICAL' for alert in alerts):
            return {
                'passed': False,
                'condition': 10,
                'reason': 'Critical monitoring alerts triggered',
                'severity': 'CRITICAL'
            }
    
    return {'passed': True, 'condition': 10}

def put_metric_data(alert):
    """Send custom metrics to CloudWatch"""
    cloudwatch = boto3.client('cloudwatch')
    
    try:
        cloudwatch.put_metric_data(
            Namespace='S3ETL',
            MetricData=[{
                'MetricName': alert['type'],
                'Value': 1,
                'Unit': 'Count',
                'Dimensions': [
                    {'Name': 'Severity', 'Value': alert['severity']},
                    {'Name': 'ExecutionId', 'Value': 'current'}
                ],
                'Timestamp': datetime.now(timezone.utc)
            }]
        )
    except Exception as e:
        logger.error(f"Failed to put CloudWatch metric: {str(e)}")

def send_sns_alert(alert):
    """Send alert via SNS"""
    sns = boto3.client('sns')
    
    try:
        sns.publish(
            TopicArn=os.environ.get('ALERT_SNS_TOPIC'),
            Subject=f"ETL Alert: {alert['type']}",
            Message=json.dumps(alert)
        )
    except Exception as e:
        logger.error(f"Failed to send SNS alert: {str(e)}")

# ============= HELPER FUNCTIONS =============
def extract_source_buckets(event):
    """Extract source buckets and keys from various event types"""
    source_map = {}
    
    # Handle S3 event trigger
    if 'Records' in event and event['Records'][0].get('s3'):
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            if bucket not in source_map:
                source_map[bucket] = []
            source_map[bucket].append(key)
    
    # Handle direct invocation with custom event
    elif 'source_buckets' in event:
        for bucket_info in event['source_buckets']:
            bucket = bucket_info['bucket']
            keys = bucket_info.get('keys', [])
            if bucket not in source_map:
                source_map[bucket] = []
            source_map[bucket].extend(keys)
    
    # Handle EventBridge scheduled event
    elif event.get('source') == 'aws.events':
        # Process all files from a manifest file
        manifest_file = event.get('manifest_file')
        if manifest_file:
            source_map = read_manifest_file(manifest_file)
    
    return source_map

def read_s3_file(bucket, key):
    """Read and parse S3 file based on extension"""
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read()
    
    # Handle compressed files
    if key.endswith('.gz'):
        content = gzip.decompress(content)
    
    # Parse based on extension
    if key.endswith('.json'):
        return json.loads(content.decode('utf-8'))
    elif key.endswith('.csv'):
        csv_content = content.decode('utf-8')
        csv_reader = csv.DictReader(io.StringIO(csv_content))
        return list(csv_reader)
    elif key.endswith('.parquet'):
        buffer = io.BytesIO(content)
        table = pq.read_table(buffer)
        return table.to_pylist()
    else:
        raise ValueError(f"Unsupported file format: {key}")

def parse_data_by_format(content, key):
    """Helper to parse data from various formats"""
    if isinstance(content, (bytes, bytearray)):
        if key.endswith('.gz'):
            content = gzip.decompress(content)
        content = content.decode('utf-8')
    
    if isinstance(content, str):
        return json.loads(content)
    elif isinstance(content, list):
        return content
    else:
        return [content]

def move_to_dlq(source_bucket, source_key, reason):
    """Move failed files to dead letter queue bucket"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    target_key = f"dlq/{timestamp}_{source_key.replace('/', '_')}"
    
    try:
        # Copy with error metadata
        s3.copy_object(
            CopySource={'Bucket': source_bucket, 'Key': source_key},
            Bucket=DEAD_LETTER_BUCKET,
            Key=target_key,
            Metadata={
                'original_bucket': source_bucket,
                'original_key': source_key,
                'failure_reason': reason,
                'failure_time': datetime.now(timezone.utc).isoformat()
            },
            MetadataDirective='REPLACE'
        )
        
        # Delete from source
        s3.delete_object(Bucket=source_bucket, Key=source_key)
        logger.info(f"Moved {source_key} to DLQ: {reason}")
        
    except Exception as e:
        logger.error(f"Failed to move to DLQ: {str(e)}")

def move_to_unprocessable_bucket(source_bucket, source_key, reason):
    """Move files that can't be processed to unprocessable bucket"""
    target_key = f"unprocessable/{source_key}"
    s3.copy_object(
        CopySource={'Bucket': source_bucket, 'Key': source_key},
        Bucket=UNPROCESSABLE_BUCKET,
        Key=target_key,
        Metadata={'failure_reason': reason}
    )

def move_to_secure_quarantine(source_bucket, source_key, reason):
    """Move security violation files to quarantine bucket with restricted access"""
    target_key = f"quarantine/{source_key}"
    s3.copy_object(
        CopySource={'Bucket': source_bucket, 'Key': source_key},
        Bucket=f"{DEAD_LETTER_BUCKET}-secure",
        Key=target_key,
        Metadata={'violation_reason': reason, 'severity': 'SECURITY'}
    )

def archive_source_file(source_bucket, source_key, execution_id):
    """Move successfully processed source file to archive"""
    archive_key = f"archived/{datetime.now().strftime('%Y/%m/%d')}/{execution_id}_{source_key.replace('/', '_')}"
    
    s3.copy_object(
        CopySource={'Bucket': source_bucket, 'Key': source_key},
        Bucket=source_bucket,
        Key=archive_key
    )
    s3.delete_object(Bucket=source_bucket, Key=source_key)

def update_processing_metadata(file_key, status, execution_id, record_count, error=None):
    """Update DynamoDB metadata after processing"""
    table = dynamodb.Table(METADATA_TABLE)
    
    update_expression = 'SET #status = :status, last_processed_time = :time, last_execution_id = :exec_id, record_count = :count'
    expression_values = {
        ':status': status,
        ':time': datetime.now(timezone.utc),
        ':exec_id': execution_id,
        ':count': record_count
    }
    expression_names = {'#status': 'status'}
    
    if error:
        update_expression += ', last_error = :error'
        expression_values[':error'] = error
    
    table.update_item(
        Key={'file_key': file_key},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_values,
        ExpressionAttributeNames=expression_names
    )

def read_manifest_file(manifest_key):
    """Read manifest file for batch processing"""
    # Implementation would parse manifest and return bucket-key mapping
    pass

def finalize_execution(results):
    """Log final execution results and clean up"""
    logger.info(f"ETL Execution completed: {json.dumps(results, default=str)}")
    
    # Send final summary to CloudWatch
    put_metric_data({
        'type': 'EXECUTION_SUMMARY',
        'message': f"Processed: {results['processed_files']}, Failed: {results['failed_files']}",
        'severity': 'INFO'
    })