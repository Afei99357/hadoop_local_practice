# Databricks notebook source
# MAGIC %md
# MAGIC # NiFi to Databricks: Equivalent Pipeline Implementation
# MAGIC 
# MAGIC This notebook demonstrates how to implement common NiFi patterns in Databricks.
# MAGIC It covers the same functionality as a typical NiFi flow but using Databricks/Spark.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import json
from datetime import datetime, timedelta

# Initialize Spark session (automatically done in Databricks)
spark = SparkSession.builder \
    .appName("NiFi_Equivalent_Pipeline") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. File Ingestion Pattern (GetFile → PutHDFS)

# COMMAND ----------

def ingest_csv_files(source_path, target_table):
    """
    Equivalent to NiFi: GetFile → ValidateRecord → ConvertRecord → PutHDFS
    """
    
    # Define schema for validation (equivalent to ValidateRecord)
    sensor_schema = StructType([
        StructField("timestamp", StringType(), False),
        StructField("sensor_id", StringType(), False),
        StructField("location", StringType(), False),
        StructField("temperature", DoubleType(), False),
        StructField("humidity", DoubleType(), False),
        StructField("pressure", DoubleType(), False),
        StructField("battery_level", IntegerType(), False),
        StructField("status", StringType(), False)
    ])
    
    # Read CSV files (equivalent to GetFile)
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .schema(sensor_schema) \
        .csv(source_path)
    
    # Add metadata (equivalent to UpdateAttribute)
    df_with_metadata = df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("data_source", lit("databricks-csv-ingestion")) \
        .withColumn("processing_date", current_date()) \
        .withColumn("file_name", input_file_name())
    
    # Validate data (equivalent to ValidateRecord)
    df_validated = df_with_metadata \
        .withColumn("is_valid",
            when(
                (col("temperature").between(-50, 50)) &
                (col("humidity").between(0, 100)) &
                (col("sensor_id").isNotNull()) &
                (col("timestamp").isNotNull()),
                True
            ).otherwise(False)
        )
    
    # Split valid and invalid records (equivalent to RouteOnAttribute)
    valid_records = df_validated.filter(col("is_valid") == True)
    invalid_records = df_validated.filter(col("is_valid") == False)
    
    # Write valid records to Delta (equivalent to PutHDFS with Parquet)
    valid_records.write \
        .mode("append") \
        .partitionBy("processing_date") \
        .format("delta") \
        .saveAsTable(target_table)
    
    # Write invalid records to error table
    if invalid_records.count() > 0:
        invalid_records.write \
            .mode("append") \
            .format("delta") \
            .saveAsTable(f"{target_table}_errors")
    
    return {
        "valid_count": valid_records.count(),
        "invalid_count": invalid_records.count(),
        "total_processed": df.count()
    }

# Example usage
result = ingest_csv_files("/mnt/data/sensor_data/*.csv", "bronze.sensor_data")
print(f"Ingestion complete: {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Streaming Ingestion Pattern (ListenHTTP → ProcessJSON → PutHDFS)

# COMMAND ----------

def create_streaming_pipeline(kafka_topic, target_table):
    """
    Equivalent to NiFi: ConsumeKafka → EvaluateJsonPath → JoltTransform → PutHDFS
    """
    
    # Read from Kafka (equivalent to ConsumeKafka)
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:9092") \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", 10000) \
        .load()
    
    # Define JSON schema
    json_schema = StructType([
        StructField("timestamp", StringType()),
        StructField("level", StringType()),
        StructField("service", StringType()),
        StructField("message", StringType()),
        StructField("trace_id", StringType()),
        StructField("response_time_ms", IntegerType())
    ])
    
    # Parse JSON (equivalent to EvaluateJsonPath)
    parsed_stream = kafka_stream.select(
        col("key").cast("string"),
        from_json(col("value").cast("string"), json_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select(
        col("data.*"),
        col("kafka_timestamp")
    )
    
    # Transform data (equivalent to JoltTransform)
    transformed_stream = parsed_stream \
        .withColumn("hour", hour(col("timestamp"))) \
        .withColumn("severity",
            when(col("level") == "CRITICAL", 5)
            .when(col("level") == "ERROR", 4)
            .when(col("level") == "WARN", 3)
            .when(col("level") == "INFO", 2)
            .otherwise(1)
        ) \
        .withColumn("alert_required",
            when(col("level").isin("CRITICAL", "ERROR"), True)
            .otherwise(False)
        )
    
    # Write to Delta with exactly-once semantics
    query = transformed_stream.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"/delta/checkpoints/{target_table}") \
        .trigger(processingTime='30 seconds') \
        .partitionBy("hour") \
        .table(target_table)
    
    return query

# Start streaming pipeline
streaming_query = create_streaming_pipeline("application_logs", "silver.app_logs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Routing Pattern (RouteOnAttribute)

# COMMAND ----------

def route_data_by_conditions(source_df):
    """
    Equivalent to NiFi: RouteOnAttribute with multiple routes
    """
    
    # Define routing conditions
    critical_alerts = source_df.filter(
        (col("level") == "CRITICAL") | 
        (col("response_time_ms") > 5000)
    )
    
    errors = source_df.filter(
        (col("level") == "ERROR") & 
        (col("response_time_ms") <= 5000)
    )
    
    warnings = source_df.filter(
        col("level") == "WARN"
    )
    
    info_logs = source_df.filter(
        col("level").isin("INFO", "DEBUG")
    )
    
    # Process each route differently
    routes = {
        "critical": (critical_alerts, "gold.critical_alerts"),
        "errors": (errors, "gold.error_logs"),
        "warnings": (warnings, "silver.warning_logs"),
        "info": (info_logs, "bronze.info_logs")
    }
    
    # Write to different destinations based on route
    for route_name, (df, table_name) in routes.items():
        if df.count() > 0:
            df.write \
                .mode("append") \
                .format("delta") \
                .saveAsTable(table_name)
            
            # Send alerts for critical issues
            if route_name == "critical":
                send_critical_alert(df)
    
    return {route: df.count() for route, (df, _) in routes.items()}

def send_critical_alert(df):
    """Send alerts for critical issues"""
    critical_count = df.count()
    if critical_count > 0:
        # In production, integrate with alerting service
        print(f"ALERT: {critical_count} critical issues detected!")
        
        # Example: Send to monitoring service
        # requests.post("https://monitoring.example.com/alerts", 
        #               json={"count": critical_count, "severity": "CRITICAL"})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Merge/Upsert Pattern (MergeContent → PutHDFS)

# COMMAND ----------

def merge_cdc_updates(source_df, target_table):
    """
    Equivalent to NiFi: MergeContent → PutHDFS with CDC handling
    """
    
    # Get target Delta table
    target = DeltaTable.forName(spark, target_table)
    
    # Deduplicate source data (equivalent to MergeContent)
    deduped_source = source_df \
        .withColumn("row_num", 
            row_number().over(
                Window.partitionBy("id").orderBy(col("timestamp").desc())
            )
        ) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    # Perform merge operation
    target.alias("target").merge(
        deduped_source.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdate(
        condition = "source.timestamp > target.timestamp",
        set = {
            "value": col("source.value"),
            "timestamp": col("source.timestamp"),
            "updated_at": current_timestamp()
        }
    ).whenNotMatchedInsert(
        values = {
            "id": col("source.id"),
            "value": col("source.value"),
            "timestamp": col("source.timestamp"),
            "created_at": current_timestamp(),
            "updated_at": current_timestamp()
        }
    ).execute()
    
    return f"Merge completed for {target_table}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Quality Monitoring (ValidateRecord → RouteOnAttribute)

# COMMAND ----------

def create_data_quality_pipeline(source_table):
    """
    Equivalent to NiFi: ValidateRecord → RouteOnAttribute → UpdateAttribute
    """
    
    # Read source data
    df = spark.table(source_table)
    
    # Define quality rules
    quality_checks = df \
        .withColumn("has_nulls", 
            col("sensor_id").isNull() | 
            col("timestamp").isNull()
        ) \
        .withColumn("temp_in_range", 
            col("temperature").between(-50, 50)
        ) \
        .withColumn("humidity_in_range", 
            col("humidity").between(0, 100)
        ) \
        .withColumn("battery_critical", 
            col("battery_level") < 10
        ) \
        .withColumn("data_quality_score",
            when(
                ~col("has_nulls") & 
                col("temp_in_range") & 
                col("humidity_in_range"),
                "GOOD"
            ).when(
                col("has_nulls") | 
                ~col("temp_in_range") | 
                ~col("humidity_in_range"),
                "POOR"
            ).otherwise("FAIR")
        )
    
    # Generate quality metrics
    metrics = quality_checks.groupBy("data_quality_score").count()
    
    # Write quality report
    quality_checks.write \
        .mode("overwrite") \
        .format("delta") \
        .saveAsTable(f"{source_table}_quality_report")
    
    # Alert on poor quality
    poor_quality_count = quality_checks.filter(col("data_quality_score") == "POOR").count()
    if poor_quality_count > 100:
        print(f"WARNING: {poor_quality_count} records with poor quality detected")
    
    return metrics.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Scheduled Job Pattern (NiFi Processor Scheduling)

# COMMAND ----------

def create_scheduled_job():
    """
    Equivalent to NiFi processor scheduling using Databricks Jobs
    """
    
    # This would be configured in Databricks Jobs UI or via API
    job_config = {
        "name": "Daily_Data_Pipeline",
        "schedule": {
            "quartz_cron_expression": "0 0 6 * * ?",  # Daily at 6 AM
            "timezone_id": "UTC"
        },
        "tasks": [
            {
                "task_key": "ingest_data",
                "notebook_task": {
                    "notebook_path": "/pipelines/ingest_daily_data",
                    "base_parameters": {
                        "date": "{{job.start_time}}",
                        "source": "s3://data-lake/raw/"
                    }
                }
            },
            {
                "task_key": "transform_data",
                "depends_on": [{"task_key": "ingest_data"}],
                "notebook_task": {
                    "notebook_path": "/pipelines/transform_data"
                }
            },
            {
                "task_key": "quality_check",
                "depends_on": [{"task_key": "transform_data"}],
                "notebook_task": {
                    "notebook_path": "/pipelines/quality_checks"
                }
            }
        ],
        "email_notifications": {
            "on_failure": ["data-team@example.com"]
        }
    }
    
    return job_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Performance Optimization Patterns

# COMMAND ----------

def optimize_pipeline_performance():
    """
    Databricks optimizations equivalent to NiFi performance tuning
    """
    
    # 1. Adaptive Query Execution (automatic in Databricks)
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    # 2. Auto-optimize Delta tables
    spark.sql("""
        ALTER TABLE bronze.sensor_data 
        SET TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
    """)
    
    # 3. Z-ordering for better query performance
    spark.sql("""
        OPTIMIZE bronze.sensor_data
        ZORDER BY (sensor_id, timestamp)
    """)
    
    # 4. Caching frequently accessed data
    frequent_data = spark.table("silver.sensor_aggregates")
    frequent_data.cache()
    
    # 5. Broadcast joins for small tables
    small_lookup = spark.table("reference.sensor_locations")
    large_data = spark.table("bronze.sensor_data")
    
    optimized_join = large_data.join(
        broadcast(small_lookup),
        "sensor_id"
    )
    
    return "Performance optimizations applied"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Monitoring and Alerting

# COMMAND ----------

def setup_monitoring():
    """
    Setup monitoring equivalent to NiFi bulletin board and monitoring
    """
    
    # Create monitoring dashboard queries
    monitoring_queries = {
        "pipeline_health": """
            SELECT 
                current_timestamp() as check_time,
                COUNT(*) as total_records,
                SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) as valid_records,
                SUM(CASE WHEN NOT is_valid THEN 1 ELSE 0 END) as invalid_records,
                AVG(response_time_ms) as avg_response_time
            FROM silver.app_logs
            WHERE timestamp > current_timestamp() - INTERVAL 1 HOUR
        """,
        
        "error_rate": """
            SELECT 
                date_trunc('hour', timestamp) as hour,
                COUNT(*) as total_requests,
                SUM(CASE WHEN level = 'ERROR' THEN 1 ELSE 0 END) as errors,
                ROUND(100.0 * SUM(CASE WHEN level = 'ERROR' THEN 1 ELSE 0 END) / COUNT(*), 2) as error_rate
            FROM silver.app_logs
            WHERE timestamp > current_timestamp() - INTERVAL 24 HOURS
            GROUP BY date_trunc('hour', timestamp)
            ORDER BY hour DESC
        """,
        
        "data_quality_trend": """
            SELECT 
                processing_date,
                data_quality_score,
                COUNT(*) as record_count
            FROM bronze.sensor_data_quality_report
            WHERE processing_date > current_date() - INTERVAL 7 DAYS
            GROUP BY processing_date, data_quality_score
            ORDER BY processing_date DESC
        """
    }
    
    # Execute monitoring queries and create alerts
    for query_name, query in monitoring_queries.items():
        result = spark.sql(query)
        
        # Check for alert conditions
        if query_name == "error_rate":
            high_error_rate = result.filter(col("error_rate") > 5.0)
            if high_error_rate.count() > 0:
                print(f"ALERT: High error rate detected in recent hours")
                # Send alert notification
    
    return "Monitoring setup complete"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC This notebook demonstrates how to implement common NiFi patterns in Databricks:
# MAGIC 
# MAGIC 1. **File Ingestion**: Auto Loader replaces GetFile/PutHDFS
# MAGIC 2. **Stream Processing**: Structured Streaming replaces ConsumeKafka/PublishKafka
# MAGIC 3. **Data Routing**: DataFrame filters replace RouteOnAttribute
# MAGIC 4. **Data Quality**: DataFrame operations replace ValidateRecord
# MAGIC 5. **Scheduling**: Databricks Jobs replace NiFi scheduling
# MAGIC 6. **Monitoring**: Databricks SQL Analytics replaces NiFi monitoring
# MAGIC 
# MAGIC Key advantages of Databricks over NiFi:
# MAGIC - Better performance at scale
# MAGIC - Native cloud integration
# MAGIC - Advanced analytics capabilities
# MAGIC - Unified batch and streaming
# MAGIC - Built-in ML capabilities