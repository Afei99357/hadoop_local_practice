# NiFi to Databricks Migration Guide

## Overview

This guide provides detailed patterns and best practices for migrating Apache NiFi data pipelines to Databricks, focusing on equivalent functionalities and architectural considerations.

## Migration Strategy

### 1. Assessment Phase
- Document existing NiFi flows and their business logic
- Identify data sources, transformations, and destinations
- Map NiFi processors to Databricks equivalents
- Analyze performance requirements and SLAs

### 2. Architecture Mapping

| NiFi Component | Databricks Component | Notes |
|----------------|---------------------|-------|
| Flow Controller | Databricks Workflows | Orchestration layer |
| Process Groups | Job Clusters | Logical grouping |
| Processors | Notebooks/Functions | Processing logic |
| FlowFiles | DataFrames | Data representation |
| Connections | Dependencies | Flow control |
| Controller Services | Cluster Libraries | Shared resources |

## Detailed Migration Patterns

### Pattern 1: Batch File Ingestion

#### NiFi Implementation
```
GetFile → UpdateAttribute → PutHDFS
```

#### Databricks Implementation
```python
# Option 1: Auto Loader (Recommended for continuous ingestion)
from pyspark.sql.functions import *

df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.inferColumnTypes", "true") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .load("/mnt/landing/sensor_data/")

# Add metadata like NiFi's UpdateAttribute
df_with_metadata = df \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("source_system", lit("sensor_network")) \
    .withColumn("processing_date", current_date())

# Write to Delta Lake (equivalent to PutHDFS)
df_with_metadata.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint/sensor_data") \
    .trigger(processingTime='1 minute') \
    .table("bronze.sensor_data")
```

### Pattern 2: REST API Data Ingestion

#### NiFi Implementation
```
ListenHTTP → EvaluateJsonPath → RouteOnAttribute → PutHDFS
```

#### Databricks Implementation
```python
# Create a Databricks Job for API ingestion
import requests
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import *

def ingest_api_data():
    """Equivalent to ListenHTTP processor"""
    
    # Initialize Spark session
    spark = SparkSession.builder.appName("APIIngestion").getOrCreate()
    
    # Fetch data from API
    response = requests.get(
        "https://api.example.com/data",
        headers={"Authorization": "Bearer " + dbutils.secrets.get("api", "token")}
    )
    
    if response.status_code == 200:
        data = response.json()
        
        # Convert to DataFrame
        df = spark.createDataFrame(data)
        
        # EvaluateJsonPath equivalent - extract fields
        df_parsed = df.select(
            col("id"),
            col("timestamp"),
            col("data.level").alias("level"),
            col("data.message").alias("message"),
            col("data.service").alias("service")
        )
        
        # RouteOnAttribute equivalent - filter and route
        error_logs = df_parsed.filter(col("level").isin("ERROR", "CRITICAL"))
        warning_logs = df_parsed.filter(col("level") == "WARN")
        info_logs = df_parsed.filter(col("level") == "INFO")
        
        # Write to different Delta tables based on routing
        error_logs.write.mode("append").saveAsTable("logs.errors")
        warning_logs.write.mode("append").saveAsTable("logs.warnings")
        info_logs.write.mode("append").saveAsTable("logs.info")
        
        return f"Processed {df.count()} records"
    else:
        raise Exception(f"API request failed: {response.status_code}")

# Schedule this as a Databricks Job
```

### Pattern 3: Kafka Stream Processing

#### NiFi Implementation
```
ConsumeKafka → ConvertAvroToJSON → JoltTransformJSON → PutHDFS
```

#### Databricks Implementation
```python
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# ConsumeKafka equivalent
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092") \
    .option("subscribe", "sensor_events") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 10000) \
    .load()

# ConvertAvroToJSON equivalent (assuming Avro schema registry)
from pyspark.sql.avro.functions import from_avro

schema_registry_url = "http://schema-registry:8081"
value_schema = fetch_schema_from_registry(schema_registry_url, "sensor_events-value")

parsed_df = kafka_df.select(
    col("key").cast("string"),
    from_avro(col("value"), value_schema).alias("data"),
    col("timestamp")
).select("key", "data.*", "timestamp")

# JoltTransformJSON equivalent - complex transformations
transformed_df = parsed_df \
    .withColumn("sensor_location", 
        concat(col("building"), lit("-"), col("floor"))) \
    .withColumn("alert_status",
        when(col("temperature") > 30, "HIGH_TEMP")
        .when(col("temperature") < 10, "LOW_TEMP")
        .otherwise("NORMAL")) \
    .withColumn("processed_timestamp", current_timestamp()) \
    .drop("building", "floor")

# Write to Delta Lake with exactly-once semantics
query = transformed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/delta/checkpoints/sensor_events") \
    .trigger(processingTime='30 seconds') \
    .table("silver.sensor_events")
```

### Pattern 4: Database CDC (Change Data Capture)

#### NiFi Implementation
```
CaptureChangeMySQL → ConvertRecord → MergeContent → PutHDFS
```

#### Databricks Implementation
```python
# Using Debezium + Kafka for CDC (preferred approach)
from delta.tables import *
from pyspark.sql.functions import *

# Read CDC events from Kafka (Debezium format)
cdc_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "mysql.inventory.customers") \
    .load()

# Parse Debezium CDC format
parsed_cdc = cdc_df.select(
    from_json(col("value").cast("string"), cdc_schema).alias("cdc")
).select(
    col("cdc.after.*"),
    col("cdc.op").alias("operation"),
    col("cdc.ts_ms").alias("timestamp")
)

# MergeContent equivalent - batch small files
batched_df = parsed_cdc \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("operation")
    ).agg(
        collect_list(struct("*")).alias("records")
    )

# Apply CDC changes to Delta table
def process_cdc_batch(batch_df, batch_id):
    """Process CDC batch and merge into target table"""
    
    target_table = DeltaTable.forName(spark, "gold.customers")
    
    # Separate operations
    inserts = batch_df.filter(col("operation") == "c")  # creates
    updates = batch_df.filter(col("operation") == "u")  # updates
    deletes = batch_df.filter(col("operation") == "d")  # deletes
    
    # Merge logic
    target_table.alias("target").merge(
        batch_df.alias("source"),
        "target.customer_id = source.customer_id"
    ).whenMatchedUpdate(
        condition = col("source.operation") == "u",
        set = {
            "name": col("source.name"),
            "email": col("source.email"),
            "updated_at": col("source.timestamp")
        }
    ).whenMatchedDelete(
        condition = col("source.operation") == "d"
    ).whenNotMatched(
        condition = col("source.operation") == "c"
    ).insertAll().execute()

# Write with foreachBatch for complex merge logic
query = batched_df.writeStream \
    .foreachBatch(process_cdc_batch) \
    .outputMode("update") \
    .trigger(processingTime='1 minute') \
    .start()
```

### Pattern 5: Data Quality & Validation

#### NiFi Implementation
```
ValidateRecord → RouteOnAttribute → UpdateAttribute → PutHDFS
                          ↓
                    PutEmail (alerts)
```

#### Databricks Implementation
```python
from pyspark.sql.functions import *
from pyspark.sql.types import *
import great_expectations as ge

# Define validation rules (ValidateRecord equivalent)
def validate_sensor_data(df):
    """Apply data quality rules"""
    
    # Create validation columns
    validated_df = df \
        .withColumn("is_valid_temperature", 
            col("temperature").between(-50, 50)) \
        .withColumn("is_valid_humidity",
            col("humidity").between(0, 100)) \
        .withColumn("is_valid_sensor_id",
            col("sensor_id").rlike("^SENS[0-9]{3}$")) \
        .withColumn("has_required_fields",
            col("sensor_id").isNotNull() & 
            col("timestamp").isNotNull() & 
            col("temperature").isNotNull())
    
    # Overall validation status
    validated_df = validated_df.withColumn(
        "validation_status",
        when(
            col("is_valid_temperature") & 
            col("is_valid_humidity") & 
            col("is_valid_sensor_id") & 
            col("has_required_fields"),
            "VALID"
        ).otherwise("INVALID")
    )
    
    # Add validation metadata (UpdateAttribute equivalent)
    return validated_df \
        .withColumn("validated_at", current_timestamp()) \
        .withColumn("validation_version", lit("v1.0"))

# Read streaming data
input_stream = spark.readStream \
    .format("delta") \
    .table("bronze.sensor_raw")

# Apply validation
validated_stream = validate_sensor_data(input_stream)

# RouteOnAttribute equivalent - separate valid and invalid records
valid_records = validated_stream.filter(col("validation_status") == "VALID")
invalid_records = validated_stream.filter(col("validation_status") == "INVALID")

# Write valid records to silver layer
valid_query = valid_records.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/delta/checkpoints/valid_sensors") \
    .table("silver.sensor_validated")

# Write invalid records to quarantine
invalid_query = invalid_records.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/delta/checkpoints/invalid_sensors") \
    .table("quarantine.sensor_invalid")

# Alert on validation failures (PutEmail equivalent)
def send_alert(batch_df, batch_id):
    """Send alerts for validation failures"""
    
    failure_count = batch_df.count()
    if failure_count > 0:
        # Send notification using Databricks alerts or external service
        alert_message = f"Data validation failed for {failure_count} records in batch {batch_id}"
        
        # Option 1: Use Databricks SQL Alerts
        # Option 2: Send to external monitoring service
        requests.post(
            "https://monitoring.example.com/alerts",
            json={"message": alert_message, "severity": "WARNING"}
        )

alert_query = invalid_records.writeStream \
    .foreachBatch(send_alert) \
    .outputMode("append") \
    .trigger(processingTime='5 minutes') \
    .start()
```

## Performance Optimization Comparison

### NiFi Optimizations → Databricks Equivalents

| NiFi Technique | Databricks Technique | Description |
|----------------|---------------------|-------------|
| Concurrent Tasks | Spark Parallelism | `spark.sql.shuffle.partitions` |
| Back Pressure | Rate Limiting | `maxOffsetsPerTrigger` for Kafka |
| Connection Pooling | Connection Cache | Spark connection pooling |
| Processor Scheduling | Trigger Intervals | `.trigger(processingTime='...')` |
| FlowFile Prioritization | Job Priorities | Cluster policies and job queues |
| Content Repository | Delta Cache | `spark.databricks.io.cache.enabled` |

## Monitoring and Observability

### NiFi Monitoring → Databricks Monitoring

```python
# Databricks stream monitoring
def monitor_stream_health(query):
    """Monitor streaming query health"""
    
    status = query.status
    progress = query.lastProgress
    
    metrics = {
        "is_active": query.isActive,
        "input_rows": progress["inputRowsPerSecond"] if progress else 0,
        "processed_rows": progress["processedRowsPerSecond"] if progress else 0,
        "batch_duration": progress["durationMs"]["triggerExecution"] if progress else 0,
        "state_operators": progress["stateOperators"] if progress else []
    }
    
    # Log metrics to monitoring system
    mlflow.log_metrics(metrics)
    
    # Alert on issues
    if not query.isActive:
        send_alert("Stream has stopped!")
    
    return metrics

# Apply monitoring
query = df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/delta/checkpoints/monitored") \
    .trigger(processingTime='1 minute') \
    .start()

# Monitor in separate thread
import threading
def continuous_monitoring():
    while True:
        monitor_stream_health(query)
        time.sleep(60)

monitor_thread = threading.Thread(target=continuous_monitoring)
monitor_thread.start()
```

## Testing Strategy

### Unit Testing Pipeline Components

```python
# Test data quality rules
def test_validation_rules():
    """Test validation logic"""
    
    # Create test data
    test_data = [
        {"sensor_id": "SENS001", "temperature": 25.0, "humidity": 45.0},  # Valid
        {"sensor_id": "INVALID", "temperature": 25.0, "humidity": 45.0},  # Invalid ID
        {"sensor_id": "SENS002", "temperature": 100.0, "humidity": 45.0}, # Invalid temp
    ]
    
    test_df = spark.createDataFrame(test_data)
    validated_df = validate_sensor_data(test_df)
    
    # Assert expectations
    valid_count = validated_df.filter(col("validation_status") == "VALID").count()
    assert valid_count == 1, f"Expected 1 valid record, got {valid_count}"
    
    invalid_count = validated_df.filter(col("validation_status") == "INVALID").count()
    assert invalid_count == 2, f"Expected 2 invalid records, got {invalid_count}"
```

## Migration Checklist

- [ ] Document all NiFi flows and their business logic
- [ ] Map NiFi processors to Databricks functions
- [ ] Identify data sources and connection requirements
- [ ] Create Databricks job definitions
- [ ] Implement error handling and retry logic
- [ ] Set up monitoring and alerting
- [ ] Migrate configuration and parameters
- [ ] Test with sample data
- [ ] Perform load testing
- [ ] Create rollback plan
- [ ] Document new architecture
- [ ] Train team on Databricks patterns

## Best Practices

1. **Use Delta Lake**: Leverage ACID transactions and time travel
2. **Implement Medallion Architecture**: Bronze → Silver → Gold layers
3. **Enable Auto-optimization**: Use Databricks auto-optimize features
4. **Version Control**: Use Databricks Repos for code management
5. **Secrets Management**: Use Databricks secrets for credentials
6. **Cost Optimization**: Use spot instances and auto-scaling
7. **Data Governance**: Implement Unity Catalog for governance

## Common Pitfalls to Avoid

1. **Over-engineering**: Don't replicate NiFi complexity unnecessarily
2. **Ignoring Cloud-Native Features**: Leverage Databricks-specific optimizations
3. **Poor State Management**: Properly handle streaming checkpoints
4. **Inadequate Testing**: Test with production-like data volumes
5. **Missing Monitoring**: Implement comprehensive observability

## Resources

- [Databricks Migration Guide](https://docs.databricks.com/migration/index.html)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Databricks Best Practices](https://docs.databricks.com/best-practices/index.html)