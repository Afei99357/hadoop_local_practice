# Databricks notebook source
# MAGIC %md
# MAGIC # Manual Translation: NiFi CSV-to-HDFS → Databricks Auto Loader to Delta
# MAGIC 
# MAGIC This is what your NiFi pipeline would look like in Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your NiFi Pipeline Translation
# MAGIC 
# MAGIC **NiFi Components:**
# MAGIC - GetFile: `/opt/nifi/nifi-current/data` → `.*\.csv` → Keep Source: true
# MAGIC - PutHDFS: → `/user/nifi/sensor_data` → Conflict: replace
# MAGIC 
# MAGIC **Databricks Equivalent:**
# MAGIC - Auto Loader: Continuous file monitoring and ingestion
# MAGIC - Delta Lake: ACID transactions and versioning

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define schema (equivalent to NiFi's file structure validation)
sensor_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("sensor_id", StringType(), True),
    StructField("location", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("battery_level", IntegerType(), True),
    StructField("status", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Auto Loader (Equivalent to NiFi GetFile)

# COMMAND ----------

# Auto Loader - Equivalent to GetFile processor
# Monitors /mnt/data/sensor_data/ for new CSV files
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", "/tmp/schema_checkpoint") \
    .option("cloudFiles.inferColumnTypes", "false") \
    .option("header", "true") \
    .schema(sensor_schema) \
    .option("cloudFiles.includeExistingFiles", "true") \
    .load("/mnt/data/sensor_data/")

# Add metadata (equivalent to NiFi UpdateAttribute)
df_with_metadata = df \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("source_file", input_file_name()) \
    .withColumn("processing_date", current_date())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Delta Lake Write (Equivalent to NiFi PutHDFS)

# COMMAND ----------

# Write to Delta Lake - Equivalent to PutHDFS processor
# With exactly-once processing and ACID guarantees
query = df_with_metadata.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint/sensor_data") \
    .option("mergeSchema", "true") \
    .trigger(processingTime='1 minute') \
    .table("sensor_data")

# Start the stream
query.start()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Error Handling (Equivalent to NiFi Failure Relationships)

# COMMAND ----------

# Enhanced version with error handling
def write_with_error_handling():
    # Validate data (equivalent to NiFi ValidateRecord)
    validated_df = df_with_metadata \
        .withColumn("is_valid", 
            when(
                (col("temperature").between(-50, 50)) &
                (col("humidity").between(0, 100)) &
                (col("sensor_id").isNotNull()),
                True
            ).otherwise(False)
        )
    
    # Split valid and invalid records
    valid_data = validated_df.filter(col("is_valid") == True)
    invalid_data = validated_df.filter(col("is_valid") == False)
    
    # Write valid data to main table
    valid_query = valid_data.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoint/valid_sensor_data") \
        .table("sensor_data_valid")
    
    # Write invalid data to quarantine table (equivalent to NiFi failure termination)
    invalid_query = invalid_data.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/checkpoint/invalid_sensor_data") \
        .table("sensor_data_quarantine")
    
    return valid_query, invalid_query

# Start error-handling pipeline
valid_q, invalid_q = write_with_error_handling()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Monitoring (Equivalent to NiFi Bulletins)

# COMMAND ----------

# Monitor stream progress
def monitor_stream():
    print("Stream Status:", query.status)
    print("Last Progress:", query.lastProgress)
    
    # Check for errors
    if not query.isActive:
        print("⚠️ Stream has stopped!")
    
    return query.lastProgress

# Check stream health
progress = monitor_stream()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Databricks Job Configuration
# MAGIC 
# MAGIC **Job Settings (equivalent to NiFi processor scheduling):**
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "CSV_to_Delta_Pipeline",
# MAGIC   "schedule": {
# MAGIC     "quartz_cron_expression": "0 */5 * * * ?",
# MAGIC     "timezone_id": "UTC"
# MAGIC   },
# MAGIC   "tasks": [
# MAGIC     {
# MAGIC       "task_key": "ingest_sensor_data",
# MAGIC       "notebook_task": {
# MAGIC         "notebook_path": "/pipelines/csv_to_delta_pipeline"
# MAGIC       }
# MAGIC     }
# MAGIC   ],
# MAGIC   "email_notifications": {
# MAGIC     "on_failure": ["data-team@company.com"]
# MAGIC   }
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: NiFi vs Databricks Mapping
# MAGIC 
# MAGIC | NiFi Component | Databricks Equivalent | Key Differences |
# MAGIC |----------------|----------------------|-----------------|
# MAGIC | GetFile | Auto Loader | Auto Loader provides schema evolution and exactly-once processing |
# MAGIC | PutHDFS | Delta Lake Writer | Delta provides ACID transactions and time travel |
# MAGIC | FlowFile | DataFrame | DataFrames are distributed and immutable |
# MAGIC | Processor Scheduling | Job Scheduler | Databricks Jobs provide better monitoring and alerting |
# MAGIC | Bulletin Board | Spark UI + Logs | More detailed performance metrics |
# MAGIC | Templates | Notebooks + Repos | Version control and collaboration built-in |
# MAGIC 
# MAGIC **Advantages of Databricks:**
# MAGIC - Better performance with Delta Lake optimization
# MAGIC - Native cloud storage integration
# MAGIC - Advanced monitoring and alerting
# MAGIC - Machine learning capabilities
# MAGIC - Collaborative development environment